// tests/core/test_ring_buffer.cpp
//
// Tests derived exclusively from SPEC.md sections 3.2 and the interface
// contract. NO implementation exists yet — this is the RED state.
//
// Spec requirements covered:
//   SPEC-3.2-SPSC-EMPTY     : try_pop on empty buffer returns false
//   SPEC-3.2-SPSC-FILL      : try_push fills buffer to exactly Capacity
//   SPEC-3.2-SPSC-FULL      : try_push on full buffer returns false, no block
//   SPEC-3.2-SPSC-FIFO      : elements arrive in push order
//   SPEC-3.2-SPSC-WRAPAROUND: correctness after many push/pop cycles
//   SPEC-3.2-SPSC-THREAD    : two-thread producer/consumer, 1M messages, no
//                             loss / duplication / reordering (TSan target)
//   SPEC-3.2-MPSC-MULTITHREAD: 4 producers + 1 consumer, per-producer order,
//                             total count correct (TSan target)
//   SPEC-3.2-MPSC-WRAPAROUND: single-threaded MPSC wraparound FIFO correctness
//                             after many push/pop cycles (full and empty boundary
//                             checked each batch cycle)
//   SPEC-3.2-POW2           : power-of-two capacity (tested on valid sizes)
//   SPEC-3.2-NOBLK          : try_push / try_pop never block (bool return)
//   SPEC-3.2-NOEXCEPT       : operations are noexcept

#include <core/ring_buffer.hpp>

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// A lightweight payload that carries a producer id and a sequence counter so
// the consumer can verify per-producer order and detect loss/duplication.
struct Item {
    std::uint32_t producer_id;
    std::uint64_t seq;
};

// ---------------------------------------------------------------------------
// SPSC — basic single-threaded contract
// ---------------------------------------------------------------------------

TEST(SpscRingBuffer, PopFromEmptyReturnsFalse) {
    // SPEC-3.2-SPSC-EMPTY
    telemetry::SpscRingBuffer<int, 8> rb;
    int val{};
    EXPECT_FALSE(rb.try_pop(val));
}

TEST(SpscRingBuffer, PushUntilCapacitySucceeds) {
    // SPEC-3.2-SPSC-FILL: exactly Capacity pushes succeed on a fresh buffer
    telemetry::SpscRingBuffer<int, 8> rb;
    for (int i = 0; i < 8; ++i) {
        EXPECT_TRUE(rb.try_push(i)) << "push " << i << " should succeed";
    }
}

TEST(SpscRingBuffer, PushBeyondCapacityReturnsFalse) {
    // SPEC-3.2-SPSC-FULL
    telemetry::SpscRingBuffer<int, 8> rb;
    for (int i = 0; i < 8; ++i) {
        rb.try_push(i);
    }
    int extra = 99;
    EXPECT_FALSE(rb.try_push(extra));
}

TEST(SpscRingBuffer, FifoOrder) {
    // SPEC-3.2-SPSC-FIFO: elements come out in push order
    telemetry::SpscRingBuffer<int, 16> rb;
    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(rb.try_push(i));
    }
    for (int i = 0; i < 10; ++i) {
        int val{};
        EXPECT_TRUE(rb.try_pop(val));
        EXPECT_EQ(val, i);
    }
}

TEST(SpscRingBuffer, PopAfterFullDrainReturnsFalse) {
    // After draining, buffer is empty again
    telemetry::SpscRingBuffer<int, 4> rb;
    for (int i = 0; i < 4; ++i) rb.try_push(i);
    for (int i = 0; i < 4; ++i) {
        int v{};
        rb.try_pop(v);
    }
    int v{};
    EXPECT_FALSE(rb.try_pop(v));
}

TEST(SpscRingBuffer, InterleavedPushPopFifo) {
    // Push 2, pop 1, push 2, pop 1 — verifies partial drain FIFO
    telemetry::SpscRingBuffer<int, 4> rb;
    EXPECT_TRUE(rb.try_push(1));
    EXPECT_TRUE(rb.try_push(2));
    int v{};
    EXPECT_TRUE(rb.try_pop(v));
    EXPECT_EQ(v, 1);
    EXPECT_TRUE(rb.try_push(3));
    EXPECT_TRUE(rb.try_push(4));
    EXPECT_TRUE(rb.try_pop(v)); EXPECT_EQ(v, 2);
    EXPECT_TRUE(rb.try_pop(v)); EXPECT_EQ(v, 3);
    EXPECT_TRUE(rb.try_pop(v)); EXPECT_EQ(v, 4);
    EXPECT_FALSE(rb.try_pop(v));
}

TEST(SpscRingBuffer, WraparoundCorrectness) {
    // SPEC-3.2-SPSC-WRAPAROUND: push/pop many more items than Capacity
    // This exercises the index-mask path many times over.
    constexpr std::size_t Cap = 16;
    constexpr int Total = 10'000;
    telemetry::SpscRingBuffer<int, Cap> rb;

    int next_push = 0;
    int next_pop  = 0;

    while (next_pop < Total) {
        // Fill as many as we can
        while (next_push < Total && rb.try_push(next_push)) {
            ++next_push;
        }
        // Drain one
        int v{};
        if (rb.try_pop(v)) {
            EXPECT_EQ(v, next_pop) << "wraparound FIFO violation at pop " << next_pop;
            ++next_pop;
        }
    }
    EXPECT_EQ(next_push, Total);
    EXPECT_EQ(next_pop,  Total);
}

TEST(SpscRingBuffer, TryPushIsNoexcept) {
    // SPEC-3.2-NOBLK / SPEC-3.2-NOEXCEPT
    telemetry::SpscRingBuffer<int, 4> rb;
    int v = 1;
    EXPECT_TRUE(noexcept(rb.try_push(v)));
}

TEST(SpscRingBuffer, TryPopIsNoexcept) {
    telemetry::SpscRingBuffer<int, 4> rb;
    int v{};
    EXPECT_TRUE(noexcept(rb.try_pop(v)));
}

// ---------------------------------------------------------------------------
// SPSC — two-thread stress test (TSan target)
// SPEC-3.2-SPSC-THREAD: 1,000,000 sequenced messages, no loss, no dup, ordered
// ---------------------------------------------------------------------------

TEST(SpscRingBuffer, TwoThreadStressOneMillion) {
    constexpr std::size_t Cap = 1024;
    constexpr std::uint64_t Total = 1'000'000ULL;

    telemetry::SpscRingBuffer<std::uint64_t, Cap> rb;

    std::atomic<bool> producer_done{false};

    // Producer: push 0..Total-1
    std::thread producer([&]() {
        for (std::uint64_t i = 0; i < Total; ++i) {
            while (!rb.try_push(i)) {
                std::this_thread::yield();
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    // Consumer: pop and verify strictly increasing sequence
    std::uint64_t received = 0;
    std::uint64_t expected = 0;
    bool order_ok = true;

    while (received < Total) {
        std::uint64_t val{};
        if (rb.try_pop(val)) {
            if (val != expected) {
                order_ok = false;
                // keep draining so producer is not stuck
            }
            ++expected;
            ++received;
        } else {
            std::this_thread::yield();
        }
    }

    producer.join();

    EXPECT_TRUE(order_ok) << "FIFO order violated in two-thread stress";
    EXPECT_EQ(received, Total) << "message count mismatch";
}

// ---------------------------------------------------------------------------
// MPSC — multiple producer threads, single consumer
// SPEC-3.2-MPSC-MULTITHREAD
// ---------------------------------------------------------------------------

TEST(MpscRingBuffer, PopFromEmptyReturnsFalse) {
    telemetry::MpscRingBuffer<int, 16> rb;
    int v{};
    EXPECT_FALSE(rb.try_pop(v));
}

TEST(MpscRingBuffer, SingleProducerFifo) {
    // Sanity: MPSC should at minimum preserve FIFO for one producer
    telemetry::MpscRingBuffer<int, 8> rb;
    for (int i = 0; i < 8; ++i) {
        EXPECT_TRUE(rb.try_push(i));
    }
    for (int i = 0; i < 8; ++i) {
        int v{};
        EXPECT_TRUE(rb.try_pop(v));
        EXPECT_EQ(v, i);
    }
}

TEST(MpscRingBuffer, PushBeyondCapacityReturnsFalse) {
    telemetry::MpscRingBuffer<int, 4> rb;
    for (int i = 0; i < 4; ++i) rb.try_push(i);
    int extra = 99;
    EXPECT_FALSE(rb.try_push(extra));
}

TEST(MpscRingBuffer, FourProducersStress) {
    // SPEC-3.2-MPSC-MULTITHREAD: 4 producers, 1 consumer.
    // Each producer pushes Items tagged with its id + a per-producer sequence.
    // Consumer verifies:
    //   (a) total count == 4 * MessagesPerProducer
    //   (b) per-producer seq is strictly increasing (no reordering within producer)
    constexpr std::size_t Cap = 1024;
    constexpr int NumProducers = 4;
    constexpr std::uint64_t MessagesPerProducer = 250'000ULL;
    constexpr std::uint64_t Total = NumProducers * MessagesPerProducer;

    telemetry::MpscRingBuffer<Item, Cap> rb;

    // Launch producers
    std::vector<std::thread> producers;
    producers.reserve(NumProducers);
    for (int pid = 0; pid < NumProducers; ++pid) {
        producers.emplace_back([&rb, pid]() {
            for (std::uint64_t seq = 0; seq < MessagesPerProducer; ++seq) {
                Item item{static_cast<std::uint32_t>(pid), seq};
                while (!rb.try_push(item)) {
                    std::this_thread::yield();
                }
            }
        });
    }

    // Consumer
    std::vector<std::uint64_t> next_expected(NumProducers, 0ULL);
    std::uint64_t received = 0;
    bool per_producer_order_ok = true;

    while (received < Total) {
        Item item{};
        if (rb.try_pop(item)) {
            auto pid = item.producer_id;
            if (pid >= static_cast<std::uint32_t>(NumProducers)) {
                per_producer_order_ok = false;
            } else if (item.seq < next_expected[pid]) {
                // duplicate or reorder within this producer
                per_producer_order_ok = false;
            } else {
                next_expected[pid] = item.seq + 1;
            }
            ++received;
        } else {
            std::this_thread::yield();
        }
    }

    for (auto& t : producers) t.join();

    EXPECT_EQ(received, Total) << "total message count mismatch";
    EXPECT_TRUE(per_producer_order_ok) << "per-producer ordering violated";

    // Each producer must have delivered all its messages
    for (int pid = 0; pid < NumProducers; ++pid) {
        EXPECT_EQ(next_expected[pid], MessagesPerProducer)
            << "producer " << pid << " delivered " << next_expected[pid]
            << " messages, expected " << MessagesPerProducer;
    }
}

TEST(MpscRingBuffer, TryPushIsNoexcept) {
    telemetry::MpscRingBuffer<int, 4> rb;
    int v = 1;
    EXPECT_TRUE(noexcept(rb.try_push(v)));
}

TEST(MpscRingBuffer, TryPopIsNoexcept) {
    telemetry::MpscRingBuffer<int, 4> rb;
    int v{};
    EXPECT_TRUE(noexcept(rb.try_pop(v)));
}

// ---------------------------------------------------------------------------
// MPSC — single-threaded wraparound correctness
// SPEC-3.2-MPSC-WRAPAROUND
//
// Pushes and pops 10,000 sequenced integers through a 16-slot buffer in
// fill-and-drain batches, forcing the internal cursors to wrap many times.
// Each batch:
//   1. Fill to capacity — verify that exactly Cap pushes succeed and the
//      (Cap+1)-th push returns false (full boundary).
//   2. Drain all Cap items — verify FIFO order and that a subsequent pop
//      returns false (empty boundary).
// ---------------------------------------------------------------------------

TEST(MpscRingBuffer, WraparoundCorrectness) {
    // SPEC-3.2-MPSC-WRAPAROUND: single-threaded push/pop of 10,000 items
    // through a 16-slot buffer in batches; verifies FIFO order, full-boundary
    // rejection, and empty-boundary rejection across many wraparound cycles.
    constexpr std::size_t Cap = 16;
    constexpr int Total = 10'000;
    telemetry::MpscRingBuffer<int, Cap> rb;

    int next_push = 0;  // next value to push
    int next_pop  = 0;  // next value expected on pop

    while (next_pop < Total) {
        // --- Fill phase: push until full or all items queued ---
        int pushed_this_batch = 0;
        while (next_push < Total && pushed_this_batch < static_cast<int>(Cap)) {
            EXPECT_TRUE(rb.try_push(next_push))
                << "push of value " << next_push << " should succeed (slot available)";
            ++next_push;
            ++pushed_this_batch;
        }

        // Full-boundary check: if we filled Cap slots and there are items
        // remaining to push, the next push must be rejected.
        if (pushed_this_batch == static_cast<int>(Cap) && next_push < Total) {
            int overflow = -1;
            EXPECT_FALSE(rb.try_push(overflow))
                << "push into full buffer (Cap=" << Cap << ") must return false";
        }

        // --- Drain phase: pop exactly what we pushed this batch ---
        for (int i = 0; i < pushed_this_batch; ++i) {
            int v{};
            EXPECT_TRUE(rb.try_pop(v))
                << "pop " << i << " of batch should succeed; next_pop=" << next_pop;
            EXPECT_EQ(v, next_pop)
                << "FIFO order violation: got " << v << " expected " << next_pop;
            ++next_pop;
        }

        // Empty-boundary check: after draining the batch the buffer is empty.
        int ghost{};
        EXPECT_FALSE(rb.try_pop(ghost))
            << "pop from empty buffer after drain must return false";
    }

    EXPECT_EQ(next_push, Total) << "total items pushed mismatch";
    EXPECT_EQ(next_pop,  Total) << "total items popped mismatch";
}

} // namespace
