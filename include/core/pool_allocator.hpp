#pragma once

#include <cstddef>
#include <cstdlib>
#include <cassert>
#include <new>

namespace telemetry {

// PoolAllocator: fixed-size block pool.
//
// All memory is reserved at construction via a single aligned_alloc call.
// allocate() and deallocate() never touch the heap; they manipulate an
// intrusive singly-linked free list stored in the blocks themselves.
//
// Block size is padded to at least alignof(std::max_align_t) so every
// returned pointer satisfies that alignment requirement (since the backing
// buffer starts aligned and each block is a multiple of that alignment).
//
// Single-threaded: no atomics; the spec says ST is acceptable for now.
class PoolAllocator {
public:
    PoolAllocator(std::size_t block_size, std::size_t block_count)
        : block_count_(block_count)
    {
        // Pad block_size up to a multiple of alignof(std::max_align_t) so
        // every block is properly aligned and there is room for the free-list
        // pointer stored intrusively in each free block.
        constexpr std::size_t kAlign = alignof(std::max_align_t);
        // block must be at least sizeof(void*) to store the free-list next ptr
        std::size_t min_size = block_size < sizeof(void*) ? sizeof(void*) : block_size;
        // round up to kAlign multiple
        stride_ = (min_size + kAlign - 1) & ~(kAlign - 1);

        // Single allocation for all blocks; aligned to kAlign.
        std::size_t total = stride_ * block_count_;
        if (total == 0) {
            storage_ = nullptr;
            free_head_ = nullptr;
            return;
        }

        storage_ = static_cast<char*>(
            ::aligned_alloc(kAlign, total));
        // Construction-time heap alloc; no heap after this point.
        if (!storage_) {
            // Allocation failure: leave the pool empty; allocate() will
            // return nullptr rather than crashing while building the list.
            free_head_ = nullptr;
            block_count_ = 0;
            return;
        }

        // Build intrusive free list: each block's first bytes hold a pointer
        // to the next free block.
        free_head_ = nullptr;
        for (std::size_t i = block_count_; i-- > 0; ) {
            void* block = storage_ + i * stride_;
            *reinterpret_cast<void**>(block) = free_head_;
            free_head_ = block;
        }
    }

    ~PoolAllocator() {
        ::free(storage_);
    }

    // Non-copyable, non-movable (owns raw memory).
    PoolAllocator(const PoolAllocator&) = delete;
    PoolAllocator& operator=(const PoolAllocator&) = delete;

    // Returns a pointer to a block of at least block_size bytes, aligned to
    // alignof(std::max_align_t). Returns nullptr if exhausted.
    // Never throws; never allocates from the heap.
    void* allocate() noexcept {
        if (!free_head_) return nullptr;
        void* block = free_head_;
        free_head_ = *reinterpret_cast<void**>(block);
        return block;
    }

    // Returns block to the pool. Behavior is undefined if p was not obtained
    // from this pool or has already been deallocated.
    void deallocate(void* p) noexcept {
        if (!p) return;
        *reinterpret_cast<void**>(p) = free_head_;
        free_head_ = p;
    }

private:
    char*       storage_{nullptr};
    void*       free_head_{nullptr};
    std::size_t stride_{0};
    std::size_t block_count_{0};
};

} // namespace telemetry
