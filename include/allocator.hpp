#ifndef DISTRIBUTED_TOKEN_BUCKET_ALLOCATOR_HPP
#define DISTRIBUTED_TOKEN_BUCKET_ALLOCATOR_HPP

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <new>
#include <system_error>
#include <atomic>
#include <array>
#include <bitset>
#include <mutex>
#include <sys/mman.h>
#include <unistd.h>

#ifndef MAP_HUGETLB
#define MAP_HUGETLB 0x40000
#endif

#ifndef MAP_HUGE_2MB
#define MAP_HUGE_2MB (21 << 26)
#endif

namespace dtb {

class AllocationException : public std::system_error {
public:
    explicit AllocationException(const std::string& what_arg)
        : std::system_error(errno, std::generic_category(), what_arg) {}
    explicit AllocationException(const char* what_arg)
        : std::system_error(errno, std::generic_category(), what_arg) {}
};

template <size_t BlockSize, size_t BlocksPerChunk>
class SlabArena {
public:
    static constexpr size_t kBlockSize = (BlockSize < sizeof(void*)) ? sizeof(void*) : BlockSize;
    static constexpr size_t kBlocksPerChunk = BlocksPerChunk;
    static constexpr size_t kChunkSize = kBlockSize * kBlocksPerChunk;

    struct ChunkHeader {
        ChunkHeader* next;
        std::atomic<size_t> free_count;
    };

    static constexpr size_t kHeaderSize = (sizeof(ChunkHeader) + kBlockSize - 1) / kBlockSize * kBlockSize;
    static constexpr size_t kActualChunkSize = kChunkSize + kHeaderSize;

    SlabArena() noexcept : free_list_(nullptr), chunk_list_(nullptr) {}

    ~SlabArena() noexcept {
        ChunkHeader* current = chunk_list_;
        while (current != nullptr) {
            ChunkHeader* next = current->next;
            int result = munmap(current, kActualChunkSize);
            if (result != 0) {
                // Destructor cannot throw; silently handle system unmap failure if needed
            }
            current = next;
        }
    }

    SlabArena(const SlabArena&) = delete;
    SlabArena& operator=(const SlabArena&) = delete;
    SlabArena(SlabArena&& other) noexcept 
        : free_list_(other.free_list_), chunk_list_(other.chunk_list_) {
        other.free_list_ = nullptr;
        other.chunk_list_ = nullptr;
    }

    SlabArena& operator=(SlabArena&& other) noexcept {
        if (this != &other) {
            this->~SlabArena();
            free_list_ = other.free_list_;
            chunk_list_ = other.chunk_list_;
            other.free_list_ = nullptr;
            other.chunk_list_ = nullptr;
        }
        return *this;
    }

    void* allocate() {
        if (free_list_ == nullptr) {
            allocate_chunk();
        }

        void* block = free_list_;
        // Read next pointer stored in the first word of the free block
        free_list_ = *static_cast<void**>(free_list_);

        // Find owning chunk header to decrement free count (optional metrics/tracking)
        // For absolute O(1), we track chunk via pointer arithmetic if needed, 
        // but simple slab layout relies on explicit free lists.
        return block;
    }

    void deallocate(void* ptr) noexcept {
        if (ptr == nullptr) {
            return;
        }
        // Link the freed block back into the thread-local free list
        *static_cast<void**>(ptr) = free_list_;
        free_list_ = ptr;
    }

private:
    void allocate_chunk() {
        // Attempt allocation using 2MB Huge Pages, fallback to standard anonymous mmap if unavailable
        void* addr = mmap(nullptr, kActualChunkSize, PROT_READ | PROT_WRITE, 
                          MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_HUGE_2MB, -1, 0);
        
        if (addr == MAP_FAILED) {
            addr = mmap(nullptr, kActualChunkSize, PROT_READ | PROT_WRITE, 
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (addr == MAP_FAILED) {
                throw AllocationException("SlabArena::allocate_chunk failed to allocate memory via mmap");
            }
        }

        // Prefault memory pages to guarantee zero allocation latency on hot path
        char* raw_chunk = static_cast<char*>(addr);
        for (size_t i = 0; i < kActualChunkSize; i += 4096) {
            volatile char c = raw_chunk[i];
            (void)c;
        }

        ChunkHeader* header = new (raw_chunk) ChunkHeader();
        header->next = chunk_list_;
        header->free_count.store(kBlocksPerChunk, std::memory_order_relaxed);
        chunk_list_ = header;

        char* block_storage = raw_chunk + kHeaderSize;
        
        // Build internal free list for the new chunk
        for (size_t i = 0; i < kBlocksPerChunk; ++i) {
            void* current_block = block_storage + (i * kBlockSize);
            void* next_block = (i < kBlocksPerChunk - 1) ? (block_storage + ((i + 1) * kBlockSize)) : free_list_;
            *static_cast<void**>(current_block) = next_block;
        }
        
        free_list_ = block_storage;
    }

    void* free_list_;
    ChunkHeader* chunk_list_;
};

class SlabAllocator {
public:
    static constexpr size_t kSmallBlockSize = 64;   // Matches Token Bucket State cache-line size
    static constexpr size_t kMediumBlockSize = 256; // Sliding-window micro-bucket tracking
    static constexpr size_t kLargeBlockSize = 1024; // Network and HLC log descriptors

    SlabAllocator() = default;
    ~SlabAllocator() = default;

    SlabAllocator(const SlabAllocator&) = delete;
    SlabAllocator& operator=(const SlabAllocator&) = delete;
    SlabAllocator(SlabAllocator&&) = default;
    SlabAllocator& operator=(SlabAllocator&&) = default;

    void* allocate(size_t size) {
        if (size <= kSmallBlockSize) {
            return small_arena_.allocate();
        } else if (size <= kMediumBlockSize) {
            return medium_arena_.allocate();
        } else if (size <= kLargeBlockSize) {
            return large_arena_.allocate();
        } else {
            // Fallback for oversized allocations exceeding predefined tiers
            void* ptr = std::malloc(size);
            if (ptr == nullptr) {
                throw AllocationException("SlabAllocator::allocate failed via std::malloc for oversized request");
            }
            return ptr;
        }
    }

    void deallocate(void* ptr, size_t size) noexcept {
        if (ptr == nullptr) {
            return;
        }
        if (size <= kSmallBlockSize) {
            small_arena_.deallocate(ptr);
        } else if (size <= kMediumBlockSize) {
            medium_arena_.deallocate(ptr);
        } else if (size <= kLargeBlockSize) {
            large_arena_.deallocate(ptr);
        } else {
            std::free(ptr);
        }
    }

    template <typename T, typename... Args>
    T* create(Args&&... args) {
        void* mem = allocate(sizeof(T));
        try {
            return new (mem) T(std::forward<Args>(args)...);
        } catch (...) {
            deallocate(mem, sizeof(T));
            throw;
        }
    }

    template <typename T>
    void destroy(T* ptr) noexcept {
        if (ptr != nullptr) {
            ptr->~T();
            deallocate(ptr, sizeof(T));
        }
    }

private:
    SlabArena<kSmallBlockSize, 1024> small_arena_;
    SlabArena<kMediumBlockSize, 512> medium_arena_;
    SlabArena<kLargeBlockSize, 256> large_arena_;
};

inline SlabAllocator& get_thread_local_slab_allocator() {
    thread_local SlabAllocator allocator;
    return allocator;
}

template <typename T>
class SlabAllocatorAdapter {
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    template <typename U>
    struct rebind {
        using other = SlabAllocatorAdapter<U>;
    };

    SlabAllocatorAdapter() noexcept = default;
    
    template <typename U>
    noexcept SlabAllocatorAdapter(const SlabAllocatorAdapter<U>&) {}

    T* allocate(std::size_t n) {
        if (n > static_cast<std::size_t>(-1) / sizeof(T)) {
            throw std::bad_array_new_length();
        }
        return static_cast<T*>(get_thread_local_slab_allocator().allocate(n * sizeof(T)));
    }

    void deallocate(T* p, std::size_t n) noexcept {
        get_thread_local_slab_allocator().deallocate(p, n * sizeof(T));
    }

    template <typename U, typename... Args>
    void construct(U* p, Args&&... args) {
        ::new (static_cast<void*>(p)) U(std::forward<Args>(args)...);
    }

    template <typename U>
    void destroy(U* p) noexcept {
        p->~U();
    }

    bool operator==(const SlabAllocatorAdapter&) const noexcept { return true; }
    bool operator!=(const SlabAllocatorAdapter&) const noexcept { return false; }
};

} // namespace dtb

#endif // DISTRIBUTED_TOKEN_BUCKET_ALLOCATOR_HPP