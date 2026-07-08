// src/feed/coinbase_live_main.cpp
//
// Live smoke/validation harness (NOT a ctest test, NOT on the hot-path
// claims). Connects to the real Coinbase Exchange WSS endpoint, subscribes to
// ticker+heartbeat for BTC-USD and ETH-USD, drives raw messages through
// CoinbaseTickerHandler into an SpscRingBuffer drained by a consumer thread,
// and prints a summary after --seconds N.
//
// Client resilience: on close/error, reconnect with simple backoff and call
// handler.on_reconnect() so the first message after resubscribe seeds a fresh
// per-stream gap baseline (no spurious gap).
//
// Build: cmake -B build -G Ninja -DBUILD_LIVE_SMOKE=ON && cmake --build build
// Run:   ./build/src/feed/coinbase_live_smoke --seconds 15

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#include <ixwebsocket/IXNetSystem.h>
#include <ixwebsocket/IXWebSocket.h>

#include <core/message.hpp>
#include <core/ring_buffer.hpp>
#include <feed/coinbase.hpp>

namespace {

constexpr char kEndpoint[] = "wss://ws-feed.exchange.coinbase.com";
constexpr char kSubscribe[] =
    R"({"type":"subscribe","product_ids":["BTC-USD","ETH-USD"],)"
    R"("channels":["ticker","heartbeat"]})";

// Ring buffer big enough that a well-behaved short run never overflows.
using Ring = telemetry::SpscRingBuffer<telemetry::Message, 1u << 16>;

struct Stats {
    std::atomic<std::uint64_t> received{0};
    std::atomic<std::uint64_t> pushed{0};
    std::atomic<std::uint64_t> parse_failures{0};
    std::atomic<std::uint64_t> gaps{0};
    std::atomic<std::uint64_t> reconnects{0};
};

}  // namespace

int main(int argc, char** argv) {
    int seconds = 15;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--seconds") == 0 && i + 1 < argc) {
            seconds = std::atoi(argv[++i]);
        }
    }

    ix::initNetSystem();

    Stats stats;
    Ring ring;
    std::atomic<bool> running{true};
    std::atomic<std::uint64_t> btc_ticks{0};
    std::atomic<std::uint64_t> eth_ticks{0};

    telemetry::feed::CoinbaseTickerHandler handler(
        {{"BTC-USD", 1u}, {"ETH-USD", 2u}},
        [&](std::uint32_t sid, std::uint64_t from, std::uint64_t to) {
            stats.gaps.fetch_add(1, std::memory_order_relaxed);
            std::cerr << "gap on stream " << sid << ": " << from << " -> " << to << '\n';
        });

    // Consumer thread: drains the ring buffer (the "processing" side).
    std::thread consumer([&] {
        telemetry::Message m;
        while (running.load(std::memory_order_acquire)) {
            while (ring.try_pop(m)) {
                if (m.stream_id == 1u) {
                    btc_ticks.fetch_add(1, std::memory_order_relaxed);
                } else if (m.stream_id == 2u) {
                    eth_ticks.fetch_add(1, std::memory_order_relaxed);
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        while (ring.try_pop(m)) {
            if (m.stream_id == 1u) btc_ticks.fetch_add(1, std::memory_order_relaxed);
            else if (m.stream_id == 2u) eth_ticks.fetch_add(1, std::memory_order_relaxed);
        }
    });

    ix::WebSocket ws;
    ws.setUrl(kEndpoint);
    // Simple reconnect/backoff (client resilience): IXWebSocket auto-reconnects;
    // we bound the backoff and resubscribe + reset gap state on each Open.
    ws.enableAutomaticReconnection();
    ws.setMinWaitBetweenReconnectionRetries(500);   // ms
    ws.setMaxWaitBetweenReconnectionRetries(5000);   // ms

    ws.setOnMessageCallback([&](const ix::WebSocketMessagePtr& msg) {
        switch (msg->type) {
        case ix::WebSocketMessageType::Open:
            // Must subscribe within 5s of connect (see docs/feeds/coinbase_ws.md).
            ws.send(kSubscribe);
            break;
        case ix::WebSocketMessageType::Message: {
            stats.received.fetch_add(1, std::memory_order_relaxed);
            const std::string& raw = msg->str;
            telemetry::Message out{};
            const telemetry::feed::ParseResult r = handler.parse(raw, out);
            if (r == telemetry::feed::ParseResult::kMalformed) {
                stats.parse_failures.fetch_add(1, std::memory_order_relaxed);
            }
            if (handler.handle(raw, ring)) {
                stats.pushed.fetch_add(1, std::memory_order_relaxed);
            }
            break;
        }
        case ix::WebSocketMessageType::Close:
        case ix::WebSocketMessageType::Error:
            // Reconnect path: forget per-stream sequence state so the first
            // message after resubscribe does not fire a spurious gap.
            stats.reconnects.fetch_add(1, std::memory_order_relaxed);
            handler.on_disconnect();
            handler.on_reconnect();
            break;
        default:
            break;
        }
    });

    std::cout << "connecting to " << kEndpoint << " for " << seconds << "s...\n";
    ws.start();
    std::this_thread::sleep_for(std::chrono::seconds(seconds));
    ws.stop();

    running.store(false, std::memory_order_release);
    consumer.join();
    ix::uninitNetSystem();

    // parse() above is called once purely to classify malformed messages for
    // the report; handle() re-parses. That double parse is fine for a smoke
    // tool (it is explicitly off the hot-path claims).
    std::cout << "\n=== coinbase_live_smoke summary ===\n";
    std::cout << "messages received : " << stats.received.load() << '\n';
    std::cout << "Messages pushed   : " << stats.pushed.load() << '\n';
    std::cout << "  BTC-USD ticks   : " << btc_ticks.load() << '\n';
    std::cout << "  ETH-USD ticks   : " << eth_ticks.load() << '\n';
    std::cout << "gap events        : " << stats.gaps.load() << '\n';
    std::cout << "parse failures    : " << stats.parse_failures.load() << '\n';
    std::cout << "reconnects        : " << stats.reconnects.load() << '\n';
    return 0;
}
