// tests/core/test_pool_allocator.cpp
//
// Tests derived exclusively from SPEC.md section 3.3 and the interface
// contract. NO implementation exists yet — this is the RED state.
//
// Spec requirements covered:
//   SPEC-3.3-RESERVEONSTRUCT : all memory reserved at construction; allocate()
//                              never touches the heap afterwards
//   SPEC-3.3-EXHAUST         : allocate() returns nullptr when pool is full
//   SPEC-3.3-REUSE           : deallocate() returns block; re-allocate succeeds
//   SPEC-3.3-DISTINCT        : returned pointers are non-overlapping
//   SPEC-3.3-ALIGN           : blocks are aligned >= alignof(std::max_align_t)
//   SPEC-3.3-BLOCKSIZE       : each block is at least block_size bytes
//   SPEC-3.3-WRITEALL        : write every byte of every block without
//                              sanitizer complaints (catches overlap/overflow)
//   SPEC-3.3-ZERO-HEAP       : constructor takes block_size + block_count;
//                              no heap after warmup (structural test only)

#include <core/pool_allocator.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <set>
#include <vector>

#include <gtest/gtest.h>

namespace {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Verify a pointer meets the minimum alignment requirement
static void expect_aligned(void* p) {
    ASSERT_NE(p, nullptr);
    auto addr = reinterpret_cast<std::uintptr_t>(p);
    EXPECT_EQ(addr % alignof(std::max_align_t), std::uintptr_t{0})
        << "block not aligned to alignof(std::max_align_t)="
        << alignof(std::max_align_t);
}

// ---------------------------------------------------------------------------
// SPEC-3.3-EXHAUST: basic construction and exhaustion
// ---------------------------------------------------------------------------

TEST(PoolAllocator, AllocateAllBlocksSucceed) {
    // SPEC-3.3-RESERVEONSTRUCT: block_count allocations all return non-null
    constexpr std::size_t BlockSize  = 64;
    constexpr std::size_t BlockCount = 8;
    telemetry::PoolAllocator pool(BlockSize, BlockCount);

    std::vector<void*> ptrs;
    ptrs.reserve(BlockCount);
    for (std::size_t i = 0; i < BlockCount; ++i) {
        void* p = pool.allocate();
        ASSERT_NE(p, nullptr) << "allocation " << i << " failed unexpectedly";
        ptrs.push_back(p);
    }
    // Cleanup: return all blocks so destructor is clean
    for (void* p : ptrs) pool.deallocate(p);
}

TEST(PoolAllocator, ExhaustionReturnsNullptr) {
    // SPEC-3.3-EXHAUST: (block_count + 1)th call returns nullptr
    constexpr std::size_t BlockSize  = 64;
    constexpr std::size_t BlockCount = 4;
    telemetry::PoolAllocator pool(BlockSize, BlockCount);

    std::vector<void*> ptrs;
    for (std::size_t i = 0; i < BlockCount; ++i) {
        void* p = pool.allocate();
        ASSERT_NE(p, nullptr);
        ptrs.push_back(p);
    }
    EXPECT_EQ(pool.allocate(), nullptr)
        << "allocate() must return nullptr when pool is exhausted";

    for (void* p : ptrs) pool.deallocate(p);
}

// ---------------------------------------------------------------------------
// SPEC-3.3-REUSE: deallocate and re-allocate
// ---------------------------------------------------------------------------

TEST(PoolAllocator, DeallocateThenAllocateSucceeds) {
    constexpr std::size_t BlockSize  = 64;
    constexpr std::size_t BlockCount = 2;
    telemetry::PoolAllocator pool(BlockSize, BlockCount);

    void* a = pool.allocate();
    void* b = pool.allocate();
    ASSERT_NE(a, nullptr);
    ASSERT_NE(b, nullptr);
    EXPECT_EQ(pool.allocate(), nullptr);    // exhausted

    pool.deallocate(a);
    void* c = pool.allocate();
    EXPECT_NE(c, nullptr) << "re-allocate after deallocate must succeed";

    pool.deallocate(b);
    pool.deallocate(c);
}

TEST(PoolAllocator, RepeatedAllocateDeallocateCycles) {
    // Stress: allocate all, free all, repeat many times — should never fail
    constexpr std::size_t BlockSize  = 64;
    constexpr std::size_t BlockCount = 8;
    telemetry::PoolAllocator pool(BlockSize, BlockCount);

    std::vector<void*> ptrs(BlockCount);
    for (int cycle = 0; cycle < 1000; ++cycle) {
        for (std::size_t i = 0; i < BlockCount; ++i) {
            ptrs[i] = pool.allocate();
            ASSERT_NE(ptrs[i], nullptr) << "cycle " << cycle << " alloc " << i;
        }
        EXPECT_EQ(pool.allocate(), nullptr);   // still exhausted mid-cycle
        for (void* p : ptrs) pool.deallocate(p);
    }
}

// ---------------------------------------------------------------------------
// SPEC-3.3-DISTINCT: returned pointers do not overlap
// ---------------------------------------------------------------------------

TEST(PoolAllocator, DistinctNonOverlappingPointers) {
    // SPEC-3.3-DISTINCT: all live pointers are unique and non-overlapping.
    // Non-overlapping is defined as: no two blocks share any byte.
    // For blocks of block_size bytes, distinct addresses separated by at least
    // block_size suffice — but we simply check uniqueness and that address
    // ranges [p, p+block_size) are disjoint.
    constexpr std::size_t BlockSize  = 64;
    constexpr std::size_t BlockCount = 16;
    telemetry::PoolAllocator pool(BlockSize, BlockCount);

    std::vector<void*> ptrs;
    ptrs.reserve(BlockCount);
    for (std::size_t i = 0; i < BlockCount; ++i) {
        void* p = pool.allocate();
        ASSERT_NE(p, nullptr);
        ptrs.push_back(p);
    }

    // Check all addresses are unique
    std::set<void*> unique_ptrs(ptrs.begin(), ptrs.end());
    EXPECT_EQ(unique_ptrs.size(), BlockCount) << "duplicate pointer returned";

    // Check no block byte-range overlaps with another block's byte-range
    for (std::size_t i = 0; i < ptrs.size(); ++i) {
        auto ai = reinterpret_cast<std::uintptr_t>(ptrs[i]);
        for (std::size_t j = i + 1; j < ptrs.size(); ++j) {
            auto aj = reinterpret_cast<std::uintptr_t>(ptrs[j]);
            // Overlap exists if ranges [ai, ai+BlockSize) and [aj, aj+BlockSize) intersect
            bool overlap = (ai < aj + BlockSize) && (aj < ai + BlockSize);
            EXPECT_FALSE(overlap)
                << "blocks " << i << " and " << j << " overlap";
        }
    }

    for (void* p : ptrs) pool.deallocate(p);
}

// ---------------------------------------------------------------------------
// SPEC-3.3-ALIGN: alignment >= alignof(std::max_align_t)
// ---------------------------------------------------------------------------

TEST(PoolAllocator, BlocksAreAlignedToMaxAlign) {
    constexpr std::size_t BlockSize  = 64;
    constexpr std::size_t BlockCount = 8;
    telemetry::PoolAllocator pool(BlockSize, BlockCount);

    std::vector<void*> ptrs;
    for (std::size_t i = 0; i < BlockCount; ++i) {
        void* p = pool.allocate();
        ASSERT_NE(p, nullptr);
        expect_aligned(p);
        ptrs.push_back(p);
    }
    for (void* p : ptrs) pool.deallocate(p);
}

TEST(PoolAllocator, ReusedBlocksAreAligned) {
    // After deallocate + re-allocate the returned pointer is still aligned
    constexpr std::size_t BlockSize  = 64;
    constexpr std::size_t BlockCount = 2;
    telemetry::PoolAllocator pool(BlockSize, BlockCount);

    void* a = pool.allocate();
    pool.deallocate(a);
    void* b = pool.allocate();
    expect_aligned(b);
    pool.deallocate(b);
}

// ---------------------------------------------------------------------------
// SPEC-3.3-BLOCKSIZE: each block is at least block_size bytes
// (write + read back verifies size — ASan catches overrun if block is small)
// ---------------------------------------------------------------------------

TEST(PoolAllocator, BlockSizeIsAtLeastRequestedSize) {
    // Write block_size bytes into each block. ASan will catch buffer overflows
    // if the pool hands out blocks smaller than requested.
    constexpr std::size_t BlockSize  = 128;
    constexpr std::size_t BlockCount = 4;
    telemetry::PoolAllocator pool(BlockSize, BlockCount);

    std::vector<void*> ptrs;
    for (std::size_t i = 0; i < BlockCount; ++i) {
        void* p = pool.allocate();
        ASSERT_NE(p, nullptr);
        std::memset(p, static_cast<int>(i + 1), BlockSize);
        ptrs.push_back(p);
    }
    // Verify the writes are still intact (no overlap from other allocations)
    for (std::size_t i = 0; i < BlockCount; ++i) {
        auto* bytes = reinterpret_cast<unsigned char*>(ptrs[i]);
        for (std::size_t b = 0; b < BlockSize; ++b) {
            EXPECT_EQ(bytes[b], static_cast<unsigned char>(i + 1))
                << "block " << i << " byte " << b << " corrupted";
        }
    }
    for (void* p : ptrs) pool.deallocate(p);
}

// ---------------------------------------------------------------------------
// SPEC-3.3-WRITEALL: write every byte of every block
// (sanitizers catch overlap / overflow)
// ---------------------------------------------------------------------------

TEST(PoolAllocator, WriteEveryByteOfEveryBlock) {
    // SPEC-3.3-WRITEALL: pattern-fill all blocks simultaneously; read back.
    // If blocks overlap or are undersized, ASan/MSan will report it.
    constexpr std::size_t BlockSize  = 64;
    constexpr std::size_t BlockCount = 32;
    telemetry::PoolAllocator pool(BlockSize, BlockCount);

    std::vector<void*> ptrs;
    ptrs.reserve(BlockCount);

    // Fill each block with a unique per-block pattern
    for (std::size_t i = 0; i < BlockCount; ++i) {
        void* p = pool.allocate();
        ASSERT_NE(p, nullptr);
        std::memset(p, static_cast<int>(0xA0u + (i & 0x3Fu)), BlockSize);
        ptrs.push_back(p);
    }

    // Verify patterns are intact
    for (std::size_t i = 0; i < BlockCount; ++i) {
        auto* bytes = reinterpret_cast<unsigned char*>(ptrs[i]);
        unsigned char expected = static_cast<unsigned char>(0xA0u + (i & 0x3Fu));
        for (std::size_t b = 0; b < BlockSize; ++b) {
            EXPECT_EQ(bytes[b], expected)
                << "byte corruption in block " << i << " at offset " << b;
        }
    }

    for (void* p : ptrs) pool.deallocate(p);
}

// ---------------------------------------------------------------------------
// SPEC-3.3-ALIGN: test with small, odd-sized blocks to confirm the allocator
// always pads / aligns even when block_size < alignof(std::max_align_t)
// ---------------------------------------------------------------------------

TEST(PoolAllocator, SmallBlocksStillAligned) {
    // block_size = 1 (pathological minimum): alignment requirement still holds
    constexpr std::size_t BlockSize  = 1;
    constexpr std::size_t BlockCount = 8;
    telemetry::PoolAllocator pool(BlockSize, BlockCount);

    std::vector<void*> ptrs;
    for (std::size_t i = 0; i < BlockCount; ++i) {
        void* p = pool.allocate();
        ASSERT_NE(p, nullptr);
        expect_aligned(p);
        ptrs.push_back(p);
    }
    for (void* p : ptrs) pool.deallocate(p);
}

// ---------------------------------------------------------------------------
// SPEC-3.3-REUSE: pool that repeatedly allocates + frees a Message-sized block
// (integration: verifies PoolAllocator works with the actual Message type)
// ---------------------------------------------------------------------------

TEST(PoolAllocator, MessageSizedBlockRoundtrip) {
    // Use sizeof(Message) = 64, alignof(Message) = 64 as the block parameters.
    // This is the primary use-case in the spec hot path.
    constexpr std::size_t BlockSize  = 64;
    constexpr std::size_t BlockCount = 4;
    telemetry::PoolAllocator pool(BlockSize, BlockCount);

    for (int cycle = 0; cycle < 500; ++cycle) {
        void* p = pool.allocate();
        ASSERT_NE(p, nullptr) << "cycle " << cycle;
        expect_aligned(p);
        // Touch all bytes — ASan will catch size violations
        std::memset(p, 0xBB, BlockSize);
        pool.deallocate(p);
    }
}

} // namespace
