// src/feed/coinbase_daemon_main.cpp
//
// 24/7 deployment daemon (SPEC 3.4 live ingestion + SPEC 3.5-3.8 detection).
// Connects to the real Coinbase Exchange WSS endpoint, subscribes to
// ticker+heartbeat for the configured products, drives raw messages through
// CoinbaseTickerHandler into an SpscRingBuffer, and runs the REAL four-layer
// detection pipeline (rules -> EWMA baseline -> CUSUM -> conformal) in the
// consumer thread. Runs until SIGTERM/SIGINT.
//
// Outputs (under --data-dir, consumed by python/api/live.py):
//   stats.json   rewritten atomically (tmp+rename) every --stats-interval-ms.
//   alerts.jsonl one JSON object per alert, append-only, fsync-free
//                (the FastAPI side syncs it into SQLite).
//
// Client resilience: IXWebSocket automatic reconnection with bounded backoff;
// on Close/Error the handler's per-stream gap state is reset so the first
// message after resubscribe seeds a fresh baseline (no spurious gap) — the
// exact behavior pinned in tests/feed/test_reconnect.cpp. The process NEVER
// exits on a feed drop; it exits only on SIGTERM/SIGINT (or startup
// misconfiguration, before connecting).
//
// Like the smoke tool, this file is deployment scaffolding and is off the
// hot-path claims: alert/stats file writes use stdio + a mutex, startup
// (config load, arg parsing) may allocate and throw. The per-message parse ->
// ring -> detect path itself reuses the audited hot-path components.

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <ixwebsocket/IXNetSystem.h>
#include <ixwebsocket/IXWebSocket.h>

#include <core/message.hpp>
#include <core/ring_buffer.hpp>
#include <detect/baseline.hpp>
#include <detect/conformal.hpp>
#include <detect/cusum.hpp>
#include <detect/rules.hpp>
#include <feed/coinbase.hpp>

namespace {

using Ring = telemetry::SpscRingBuffer<telemetry::Message, 1u << 16>;

constexpr char kDefaultEndpoint[] = "wss://ws-feed.exchange.coinbase.com";

std::atomic<bool> g_running{true};

void handle_signal(int /*sig*/) { g_running.store(false, std::memory_order_release); }

struct Counters {
    std::atomic<std::uint64_t> received{0};
    std::atomic<std::uint64_t> pushed{0};
    std::atomic<std::uint64_t> processed{0};
    std::atomic<std::uint64_t> parse_failures{0};
    std::atomic<std::uint64_t> gaps{0};
    std::atomic<std::uint64_t> reconnects{0};
    std::atomic<std::uint64_t> alerts{0};
    // Wall-clock milliseconds when the last message arrived; exchange
    // timestamp (ns) of the last accepted tick. 0 = none yet.
    std::atomic<std::int64_t> last_message_unix_ms{0};
    std::atomic<std::int64_t> last_message_ts_ns{0};
};

std::int64_t now_unix_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// "2026-07-08T12:34:56.789Z" from unix milliseconds.
std::string iso8601_utc(std::int64_t unix_ms) {
    const std::time_t secs = static_cast<std::time_t>(unix_ms / 1000);
    std::tm tm{};
    gmtime_r(&secs, &tm);
    char buf[40];
    const std::size_t n = std::strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tm);
    std::snprintf(buf + n, sizeof(buf) - n, ".%03dZ", static_cast<int>(unix_ms % 1000));
    return buf;
}

// Append-only JSONL alert sink. Mutex-protected: alerts originate from both
// the consumer thread (detection layers) and the WS thread (gap callback).
class AlertWriter {
public:
    bool open(const std::string& path) {
        file_ = std::fopen(path.c_str(), "a");
        return file_ != nullptr;
    }

    ~AlertWriter() {
        if (file_ != nullptr) {
            std::fclose(file_);
        }
    }

    void write(std::uint32_t stream_id, std::int64_t ts_ns, const char* layer,
               const std::string& detail) {
        const std::string created_at = iso8601_utc(now_unix_ms());
        std::lock_guard<std::mutex> lock(mu_);
        std::fprintf(file_,
                     "{\"stream_id\":%u,\"ts_ns\":%lld,\"layer\":\"%s\","
                     "\"detail\":\"%s\",\"created_at\":\"%s\"}\n",
                     stream_id, static_cast<long long>(ts_ns), layer,
                     detail.c_str(), created_at.c_str());
        std::fflush(file_);
    }

private:
    std::FILE* file_ = nullptr;
    std::mutex mu_;
};

// Atomically (tmp + rename) rewrite stats.json.
void write_stats(const std::string& dir, const Counters& c, double start_unix_s,
                 const std::vector<std::uint32_t>& streams) {
    const std::string tmp = dir + "/stats.json.tmp";
    const std::string dst = dir + "/stats.json";
    std::FILE* f = std::fopen(tmp.c_str(), "w");
    if (f == nullptr) {
        return;  // transient FS problem must not kill the daemon
    }
    std::string streams_json = "[";
    for (std::size_t i = 0; i < streams.size(); ++i) {
        if (i > 0) streams_json += ",";
        streams_json += std::to_string(streams[i]);
    }
    streams_json += "]";
    std::fprintf(
        f,
        "{\"start_time_unix\":%.3f,\"now_unix\":%.3f,"
        "\"messages_received\":%llu,\"messages_processed\":%llu,"
        "\"ticks_pushed\":%llu,\"parse_failures\":%llu,\"gaps\":%llu,"
        "\"reconnects\":%llu,\"alerts\":%llu,"
        "\"last_message_unix_ms\":%lld,\"last_message_ts_ns\":%lld,"
        "\"streams\":%s}\n",
        start_unix_s, static_cast<double>(now_unix_ms()) / 1000.0,
        static_cast<unsigned long long>(c.received.load(std::memory_order_relaxed)),
        static_cast<unsigned long long>(c.processed.load(std::memory_order_relaxed)),
        static_cast<unsigned long long>(c.pushed.load(std::memory_order_relaxed)),
        static_cast<unsigned long long>(c.parse_failures.load(std::memory_order_relaxed)),
        static_cast<unsigned long long>(c.gaps.load(std::memory_order_relaxed)),
        static_cast<unsigned long long>(c.reconnects.load(std::memory_order_relaxed)),
        static_cast<unsigned long long>(c.alerts.load(std::memory_order_relaxed)),
        static_cast<long long>(c.last_message_unix_ms.load(std::memory_order_relaxed)),
        static_cast<long long>(c.last_message_ts_ns.load(std::memory_order_relaxed)),
        streams_json.c_str());
    std::fclose(f);
    std::rename(tmp.c_str(), dst.c_str());
}

struct Options {
    std::string data_dir = "./data";
    std::string endpoint = kDefaultEndpoint;
    std::vector<std::string> rule_paths;
    // product_id -> stream_id. Defaults match config/crypto_ticks.yaml
    // (BTC-USD = 10) and config/crypto_eth_usd.yaml (ETH-USD = 11).
    std::unordered_map<std::string, std::uint32_t> products;
    int stats_interval_ms = 1000;
};

bool parse_args(int argc, char** argv, Options& opt) {
    for (int i = 1; i < argc; ++i) {
        const std::string a = argv[i];
        auto next = [&](const char* flag) -> const char* {
            if (i + 1 >= argc) {
                std::cerr << flag << " requires a value\n";
                return nullptr;
            }
            return argv[++i];
        };
        if (a == "--data-dir") {
            const char* v = next("--data-dir");
            if (v == nullptr) return false;
            opt.data_dir = v;
        } else if (a == "--endpoint") {
            const char* v = next("--endpoint");
            if (v == nullptr) return false;
            opt.endpoint = v;
        } else if (a == "--rules") {
            const char* v = next("--rules");
            if (v == nullptr) return false;
            opt.rule_paths.push_back(v);
        } else if (a == "--product") {  // e.g. --product BTC-USD=10
            const char* v = next("--product");
            if (v == nullptr) return false;
            const std::string s = v;
            const auto eq = s.find('=');
            if (eq == std::string::npos) {
                std::cerr << "--product expects NAME=STREAM_ID, got: " << s << '\n';
                return false;
            }
            opt.products[s.substr(0, eq)] =
                static_cast<std::uint32_t>(std::strtoul(s.c_str() + eq + 1, nullptr, 10));
        } else if (a == "--stats-interval-ms") {
            const char* v = next("--stats-interval-ms");
            if (v == nullptr) return false;
            opt.stats_interval_ms = std::atoi(v);
        } else {
            std::cerr << "unknown argument: " << a << '\n';
            return false;
        }
    }
    if (opt.products.empty()) {
        opt.products = {{"BTC-USD", 10u}, {"ETH-USD", 11u}};
    }
    return true;
}

std::string build_subscribe(const Options& opt) {
    std::string ids;
    for (const auto& [product, sid] : opt.products) {
        (void)sid;
        if (!ids.empty()) ids += ",";
        ids += "\"" + product + "\"";
    }
    return R"({"type":"subscribe","product_ids":[)" + ids +
           R"(],"channels":["ticker","heartbeat"]})";
}

}  // namespace

int main(int argc, char** argv) {
    Options opt;
    if (!parse_args(argc, argv, opt)) {
        return 2;
    }

    // Startup (may throw / allocate; nothing is connected yet). A bad rules
    // file is a config error: fail loudly now rather than run rule-less.
    telemetry::detect::RuleChecker rules;
    for (const std::string& path : opt.rule_paths) {
        try {
            rules.add_rule(telemetry::detect::load_rule_config(path));
        } catch (const std::exception& e) {
            std::cerr << "failed to load rules from " << path << ": " << e.what() << '\n';
            return 2;
        }
    }

    std::vector<std::uint32_t> stream_ids;
    for (const auto& [product, sid] : opt.products) {
        (void)product;
        stream_ids.push_back(sid);
    }

    AlertWriter alerts;
    if (!alerts.open(opt.data_dir + "/alerts.jsonl")) {
        std::cerr << "cannot open " << opt.data_dir
                  << "/alerts.jsonl (does --data-dir exist?)\n";
        return 2;
    }

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    ix::initNetSystem();

    Counters counters;
    Ring ring;
    const double start_unix_s = static_cast<double>(now_unix_ms()) / 1000.0;

    telemetry::feed::CoinbaseTickerHandler handler(
        opt.products,
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            counters.gaps.fetch_add(1, std::memory_order_relaxed);
            counters.alerts.fetch_add(1, std::memory_order_relaxed);
            alerts.write(sid, 0, "feed",
                         "gap seq " + std::to_string(from) + "->" + std::to_string(to));
        });

    // Consumer thread: drains the ring and runs the real detection pipeline
    // (same layer order and out-of-sample semantics as python/api/engine.py).
    std::thread consumer([&] {
        telemetry::detect::EwmaBaseline baseline(0.05);
        telemetry::detect::CusumDetector cusum(0.01, 3.0, 8.0);
        telemetry::detect::ConformalThreshold conformal(500);
        constexpr double kConformalAlpha = 0.01;

        telemetry::Message m;
        while (g_running.load(std::memory_order_acquire)) {
            bool worked = false;
            while (ring.try_pop(m)) {
                worked = true;
                counters.processed.fetch_add(1, std::memory_order_relaxed);
                counters.last_message_ts_ns.store(m.ts_ns, std::memory_order_relaxed);

                const auto rr = rules.check(m.stream_id, m.value, m.ts_ns);
                if (rr != telemetry::detect::RuleResult::kOk) {
                    counters.alerts.fetch_add(1, std::memory_order_relaxed);
                    alerts.write(m.stream_id, m.ts_ns, "rules",
                                 rr == telemetry::detect::RuleResult::kOutOfBounds
                                     ? "out_of_bounds"
                                     : "rate_violation");
                }

                const double z = baseline.zscore(m.stream_id, m.value);
                baseline.update(m.stream_id, m.value);
                const double score = z < 0.0 ? -z : z;

                if (cusum.update_and_check(m.stream_id, m.value)) {
                    counters.alerts.fetch_add(1, std::memory_order_relaxed);
                    alerts.write(m.stream_id, m.ts_ns, "cusum", "changepoint");
                }

                if (conformal.is_anomalous(m.stream_id, score, kConformalAlpha)) {
                    char detail[64];
                    std::snprintf(detail, sizeof(detail), "score=%.4f>thr=%.4f", score,
                                  conformal.threshold(m.stream_id, kConformalAlpha));
                    counters.alerts.fetch_add(1, std::memory_order_relaxed);
                    alerts.write(m.stream_id, m.ts_ns, "conformal", detail);
                }
                conformal.update(m.stream_id, score);
            }
            if (!worked) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        }
    });

    // Stats thread: periodic atomic rewrite of stats.json.
    std::thread stats_writer([&] {
        while (g_running.load(std::memory_order_acquire)) {
            write_stats(opt.data_dir, counters, start_unix_s, stream_ids);
            std::this_thread::sleep_for(std::chrono::milliseconds(opt.stats_interval_ms));
        }
    });

    ix::WebSocket ws;
    ws.setUrl(opt.endpoint);
    ws.enableAutomaticReconnection();          // NEVER exit on a feed drop
    ws.setMinWaitBetweenReconnectionRetries(500);   // ms
    ws.setMaxWaitBetweenReconnectionRetries(10000); // ms
    // Keepalive: a silently black-holed TCP connection (NAT timeout, VPS
    // network blip) would otherwise stall the feed without an Error/Close
    // event; failed pings surface it and trigger the reconnect path.
    ws.setPingInterval(10);  // seconds

    const std::string subscribe = build_subscribe(opt);
    ws.setOnMessageCallback([&](const ix::WebSocketMessagePtr& msg) {
        switch (msg->type) {
        case ix::WebSocketMessageType::Open:
            std::cout << "connected, subscribing: " << subscribe << std::endl;
            ws.send(subscribe);
            break;
        case ix::WebSocketMessageType::Message: {
            counters.received.fetch_add(1, std::memory_order_relaxed);
            counters.last_message_unix_ms.store(now_unix_ms(), std::memory_order_relaxed);
            // Same parse-for-classification + handle pattern as the smoke
            // tool: the double parse is fine off the hot-path claims.
            telemetry::Message probe{};
            if (handler.parse(msg->str, probe) ==
                telemetry::feed::ParseResult::kMalformed) {
                counters.parse_failures.fetch_add(1, std::memory_order_relaxed);
            }
            if (handler.handle(msg->str, ring)) {
                counters.pushed.fetch_add(1, std::memory_order_relaxed);
            }
            break;
        }
        case ix::WebSocketMessageType::Close:
        case ix::WebSocketMessageType::Error:
            // Feed drop: reset per-stream gap state so the first message after
            // resubscribe seeds a fresh baseline (tests/feed/test_reconnect.cpp).
            // The Close fired by ws.stop() during shutdown is not a reconnect.
            if (g_running.load(std::memory_order_acquire)) {
                counters.reconnects.fetch_add(1, std::memory_order_relaxed);
            }
            handler.on_disconnect();
            handler.on_reconnect();
            break;
        default:
            break;
        }
    });

    std::cout << "coinbase_daemon: endpoint=" << opt.endpoint
              << " data_dir=" << opt.data_dir << " products=" << subscribe << std::endl;
    ws.start();

    while (g_running.load(std::memory_order_acquire)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    std::cout << "shutting down (signal received)" << std::endl;
    ws.stop();
    consumer.join();
    stats_writer.join();
    write_stats(opt.data_dir, counters, start_unix_s, stream_ids);  // final snapshot
    ix::uninitNetSystem();
    return 0;
}
