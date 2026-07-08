#pragma once

#include <atomic>
#include <array>
#include <cstddef>
#include <type_traits>

namespace telemetry {

// ---------------------------------------------------------------------------
// SpscRingBuffer<T, Capacity>
//
// Single-producer, single-consumer lock-free ring buffer.
//
// Memory ordering rationale:
//   - Producer writes T into storage, then publishes with release on tail_.
//     This ensures the written data is visible to the consumer before the
//     updated tail is observed.
//   - Consumer reads tail_ with acquire, which synchronizes with the
//     producer's release store and ensures data is visible.
//   - Symmetric acquire/release on head_ for consumer -> producer direction.
//   - No seq_cst is used: acquire/release pairs are sufficient for the
//     single-producer / single-consumer case and are cheaper on x86/ARM.
//
// Capacity must be a power of two; enforced by static_assert.
// Unbounded monotonically increasing head/tail counters masked on access
// allow full Capacity occupancy (size == tail - head, full when == Capacity).
// ---------------------------------------------------------------------------
template<typename T, std::size_t Capacity>
class SpscRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0,
        "SpscRingBuffer: Capacity must be a power of two");
    static_assert(Capacity > 0, "SpscRingBuffer: Capacity must be non-zero");

    static constexpr std::size_t kMask = Capacity - 1;

public:
    SpscRingBuffer() noexcept : head_(0), tail_(0) {}

    // Returns true if item was pushed; false if buffer is full.
    bool try_push(const T& item) noexcept {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        const std::size_t head = head_.load(std::memory_order_acquire);
        if (tail - head >= Capacity) {
            return false;  // full
        }
        storage_[tail & kMask] = item;
        // Release: make the written data visible before the tail update.
        tail_.store(tail + 1, std::memory_order_release);
        return true;
    }

    // Returns true if an item was popped into out; false if buffer is empty.
    bool try_pop(T& out) noexcept {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        if (head == tail) {
            return false;  // empty
        }
        out = storage_[head & kMask];
        // Release: make consumption visible before head update.
        head_.store(head + 1, std::memory_order_release);
        return true;
    }

private:
    // Pad head and tail to separate cache lines to prevent false sharing.
    alignas(64) std::atomic<std::size_t> head_;
    alignas(64) std::atomic<std::size_t> tail_;

    std::array<T, Capacity> storage_;
};


// ---------------------------------------------------------------------------
// MpscRingBuffer<T, Capacity>
//
// Multiple-producer, single-consumer lock-free ring buffer.
//
// Design: producers claim a slot via a CAS loop on write_cursor_ (checking
// the slot's sequence first so a full buffer is detected without claiming),
// write data into the slot, then mark the slot as ready by storing a
// per-slot sequence number. The consumer checks the sequence of the next
// expected slot before reading.
//
// Memory ordering rationale:
//   - Producers use compare_exchange_weak(relaxed) to claim a slot; relaxed
//     is safe because the RMW's atomicity alone provides mutual exclusion
//     for slot claiming — payload ordering comes from the slot sequence.
//   - After writing the slot, the producer does a release store to the slot's
//     sequence counter so the consumer's acquire load synchronizes with it.
//   - The consumer does acquire load on the sequence counter; if it matches
//     the expected value the data is ready. After reading, the consumer does
//     a relaxed store to advance read_cursor_ (only one consumer, no contention).
//   - No seq_cst is used; the acquire/release pair on the per-slot sequence
//     counter provides the necessary happens-before guarantee without the
//     full fence overhead of seq_cst.
//
// Capacity must be a power of two. Full occupancy (Capacity items) is
// supported because the wrap-around check uses write_cursor_ - read_cursor_.
// ---------------------------------------------------------------------------
template<typename T, std::size_t Capacity>
class MpscRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0,
        "MpscRingBuffer: Capacity must be a power of two");
    static_assert(Capacity > 0, "MpscRingBuffer: Capacity must be non-zero");

    static constexpr std::size_t kMask = Capacity - 1;

    struct Slot {
        T data;
        // sequence == slot_index means ready to read.
        // Initially slot i has sequence i (ready for producer to claim,
        // but we use a different sentinel: sequence i means producer i*Capacity
        // can write). We use the classic Dmitry Vyukov MPMC approach adapted
        // for MPSC: sequence[i] starts at i; producer claiming position p
        // expects sequence[p & mask] == p, then writes, then sets seq to p+1.
        // Consumer at position r expects sequence[r & mask] == r+1, reads,
        // then sets seq to r + Capacity.
        // NOT cache-line padded: data and sequence of one slot are touched
        // by one thread at a time (handoff via acquire/release); padding every
        // slot would double memory (128 B/slot for Message) for no benefit.
        // Only the cursors below need false-sharing isolation.
        std::atomic<std::size_t> sequence;
    };

public:
    MpscRingBuffer() noexcept {
        for (std::size_t i = 0; i < Capacity; ++i) {
            slots_[i].sequence.store(i, std::memory_order_relaxed);
        }
        write_cursor_.store(0, std::memory_order_relaxed);
        read_cursor_.store(0, std::memory_order_relaxed);
    }

    bool try_push(const T& item) noexcept {
        std::size_t pos = write_cursor_.load(std::memory_order_relaxed);
        for (;;) {
            Slot& slot = slots_[pos & kMask];
            std::size_t seq = slot.sequence.load(std::memory_order_acquire);
            std::ptrdiff_t diff = static_cast<std::ptrdiff_t>(seq)
                                - static_cast<std::ptrdiff_t>(pos);
            if (diff == 0) {
                // Slot is available; try to claim it.
                if (write_cursor_.compare_exchange_weak(
                        pos, pos + 1,
                        std::memory_order_relaxed,
                        std::memory_order_relaxed)) {
                    // We own this slot; write data and publish.
                    slot.data = item;
                    // Release: consumer's acquire on sequence synchronizes here.
                    slot.sequence.store(pos + 1, std::memory_order_release);
                    return true;
                }
                // CAS failed: another producer grabbed pos; retry with new pos.
            } else if (diff < 0) {
                // Buffer is full (slot not yet consumed).
                return false;
            } else {
                // Another producer already moved write_cursor_ past pos;
                // reload and retry.
                pos = write_cursor_.load(std::memory_order_relaxed);
            }
        }
    }

    bool try_pop(T& out) noexcept {
        const std::size_t pos = read_cursor_.load(std::memory_order_relaxed);
        Slot& slot = slots_[pos & kMask];
        const std::size_t seq = slot.sequence.load(std::memory_order_acquire);
        // Producer sets sequence to pos+1 after writing; we expect exactly pos+1.
        if (seq != pos + 1) {
            return false;  // not ready yet or empty
        }
        out = slot.data;
        // Recycle slot: set sequence to pos + Capacity so a future producer
        // at position pos+Capacity will see diff==0 and can claim it.
        slot.sequence.store(pos + Capacity, std::memory_order_release);
        read_cursor_.store(pos + 1, std::memory_order_relaxed);
        return true;
    }

private:
    alignas(64) std::atomic<std::size_t> write_cursor_;
    alignas(64) std::atomic<std::size_t> read_cursor_;

    std::array<Slot, Capacity> slots_;
};

} // namespace telemetry
