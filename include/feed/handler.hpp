#pragma once

// feed/handler.hpp
//
// SPEC-3.4: WebSocket ingestion -> parse -> Message -> ring buffer, with
// per-stream sequence-gap detection and reconnect/resync.
//
// FeedHandler is the generic (adapter-agnostic) base. It owns:
//   - the pinned ParseResult vocabulary,
//   - the per-stream_id gap-detection state machine (shared by ticker and
//     heartbeat events; see D-010 for why Message.seq == trade_id),
//   - the reconnect/resync lifecycle (on_disconnect/on_reconnect/reset),
//   - the handle<RingBuffer>() template that ties parse() + gap detection +
//     push together.
//
// Adapters (e.g. CoinbaseTickerHandler in feed/coinbase.hpp) implement only
// parse().
//
// Hot-path notes:
//   - parse() is noexcept (no exceptions on the ingest path, per CLAUDE.md /
//     cpp-conventions). handle() is noexcept.
//   - The gap-state map inserts only the first time each stream_id is seen
//     (bounded by the number of streams, i.e. warmup); steady-state lookups
//     do not allocate. See DECISIONS D-011.

#include <cstdint>
#include <functional>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <core/message.hpp>

namespace telemetry::feed {

// Pinned parse outcome vocabulary (see test_coinbase_parse.cpp):
//   kOk        : a real tick -> fully-populated Message, pushed to the buffer.
//   kHeartbeat : Message.stream_id/seq populated for gap tracking ONLY; never
//                pushed.
//   kSkipped   : recognized-but-irrelevant (ack, unmapped product, unknown
//                but valid JSON type). No gap-state effect.
//   kMalformed : invalid JSON / missing required field. No gap-state effect.
enum class ParseResult : std::uint8_t { kOk, kHeartbeat, kSkipped, kMalformed };

// on_gap(stream_id, from, to): a gap was detected on stream_id between the
// last accepted seq (`from`) and the incoming seq (`to`), i.e. seq values in
// (from, to) were missed. Fires exactly once per gapped message.
using GapCallback =
    std::function<void(std::uint32_t stream_id, std::uint64_t from, std::uint64_t to)>;

class FeedHandler {
public:
    explicit FeedHandler(GapCallback on_gap = nullptr) noexcept
        : on_gap_(std::move(on_gap)) {}

    virtual ~FeedHandler() = default;

    FeedHandler(const FeedHandler&) = delete;
    FeedHandler& operator=(const FeedHandler&) = delete;

    // Per-adapter parse. Fills `out` on kOk (full tick) or kHeartbeat
    // (stream_id + seq only). Never throws.
    virtual ParseResult parse(std::string_view raw, Message& out) noexcept = 0;

    // Parse `raw`, run gap detection, and push to `buffer` when appropriate.
    // Returns true iff a Message was pushed to the buffer.
    //   kOk        : gap-checked; pushed unless it is a duplicate/older seq
    //                (dropped) or the buffer is full.
    //   kHeartbeat : gap-checked (may fire on_gap) but never pushed.
    //   kSkipped / kMalformed : ignored, no gap-state change.
    template <typename RingBuffer>
    bool handle(std::string_view raw, RingBuffer& buffer) noexcept {
        Message out{};
        const ParseResult r = parse(raw, out);
        switch (r) {
        case ParseResult::kOk:
            if (!accept_seq(out.stream_id, out.seq)) {
                return false;  // duplicate / out-of-order-older: dropped
            }
            return buffer.try_push(out);
        case ParseResult::kHeartbeat:
            // Participates in gap detection but is never pushed.
            accept_seq(out.stream_id, out.seq);
            return false;
        case ParseResult::kSkipped:
        case ParseResult::kMalformed:
            return false;
        }
        return false;
    }

    // Lifecycle. on_disconnect() is a no-op marker; the resync happens on
    // on_reconnect(), which is exactly reset(): all per-stream expected-seq
    // state is forgotten so the first post-reconnect message per stream seeds
    // a fresh baseline without a spurious gap (see test_reconnect.cpp).
    void on_disconnect() noexcept {}
    void on_reconnect() noexcept { reset(); }
    void reset() noexcept { last_seq_.clear(); }

private:
    // Gap-detection state machine for one stream_id. Returns true if the
    // message should be accepted (pushed by the caller for kOk), false if it
    // is a duplicate / out-of-order-older seq that must be dropped. Fires
    // on_gap() as a side effect when seq skips ahead.
    bool accept_seq(std::uint32_t stream_id, std::uint64_t seq) noexcept {
        auto it = last_seq_.find(stream_id);
        if (it == last_seq_.end()) {
            last_seq_.emplace(stream_id, seq);  // first message for this stream
            return true;
        }
        const std::uint64_t last = it->second;
        if (seq <= last) {
            return false;  // duplicate or out-of-order-older: drop, no update
        }
        if (seq > last + 1 && on_gap_) {
            on_gap_(stream_id, last, seq);
        }
        it->second = seq;
        return true;
    }

    GapCallback on_gap_;
    std::unordered_map<std::uint32_t, std::uint64_t> last_seq_;
};

}  // namespace telemetry::feed
