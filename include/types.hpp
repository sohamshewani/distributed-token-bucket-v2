#ifndef DISTRIBUTED_TOKEN_BUCKET_TYPES_HPP
#define DISTRIBUTED_TOKEN_BUCKET_TYPES_HPP

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <new>
#include <system_error>
#include <chrono>
#include <array>
#include <immintrin.h>
#include <sys/mman.h>
#include <unistd.h>

namespace distributed_token_bucket {

enum class ErrorCode : uint32_t {
    Success = 0,
    RateLimitExceeded = 1,
    CircuitBreakerTripped = 2,
    BufferFull = 3,
    AllocationFailed = 4,
    ClockSkewOutOfBounds = 5,
    InvalidConfiguration = 6,
    StorageError = 7
};

class TokenBucketException : public std::system_error {
public:
    explicit TokenBucketException(ErrorCode err, const char* message)
        : std::system_error(static_origin(err), message), err_(err) {}

    [[nodiscard]] ErrorCode error_code() const noexcept { return err_; }

private:
    static std::error_code static_origin(ErrorCode err) {
        return std::error_code(static_cast<int>(err), std::generic_category());
    }
    ErrorCode err_;
};

// Cache-line aligned and packed 64-byte token bucket state to prevent false sharing
struct alignas(64) TokenBucketState {
    std::atomic<uint64_t> tokens;
    std::atomic<uint64_t> last_update_ns;
    uint32_t capacity_scaled;
    uint32_t refill_rate_per_ns_scaled;
    uint32_t node_id;
    uint32_t padding_to_64_bytes[3]; // Fills remaining bytes to exactly 64 bytes
};

static_assert(sizeof(TokenBucketState) == 64, "TokenBucketState must be exactly 64 bytes for cache line isolation");

// Sliding-window log ring buffer of micro-bucket counters using atomic fetch-add
template <size_t Capacity = 64>
class SlidingWindowLog {
public:
    SlidingWindowLog() noexcept {
        for (auto& bucket : buckets_) {
            bucket.store(0, std::memory_order_relaxed);
        }
        head_index_.store(0, std::memory_order_relaxed);
    }

    ~SlidingWindowLog() = default;

    void record_event(uint64_t current_time_ns, uint64_t window_size_ns, uint64_t count) noexcept {
        uint64_t bucket_duration = window_size_ns / Capacity;
        if (bucket_duration == 0) bucket_duration = 1;
        
        size_t target_index = (current_time_ns / bucket_duration) % Capacity;
        
        // Optimistic atomic accumulation
        buckets_[target_index].fetch_add(count, std::memory_order_relaxed);
    }

    [[nodiscard]] uint64_t get_window_sum(uint64_t current_time_ns, uint64_t window_size_ns) noexcept {
        uint64_t bucket_duration = window_size_ns / Capacity;
        if (bucket_duration == 0) bucket_duration = 1;

        uint64_t current_bucket_time = current_time_ns / bucket_duration;
        uint64_t total = 0;

        for (size_t i = 0; i < Capacity; ++i) {
            uint64_t bucket_time = (current_bucket_time >= i) ? (current_bucket_time - i) : 0;
            if ((current_bucket_time - bucket_time) < Capacity) {
                size_t index = bucket_time % Capacity;
                total += buckets_[index].load(std::memory_order_relaxed);
            }
        }
        return total;
    }

    void reset() noexcept {
        for (auto& bucket : buckets_) {
            bucket.store(0, std::memory_order_relaxed);
        }
    }

private:
    std::array<std::atomic<uint64_t>, Capacity> buckets_;
    std::atomic<uint64_t> head_index_;
};

// Zero-allocation hot path Slab Allocator backed by huge pages (MAP_HUGETLB)
class SlabAllocator {
public:
    explicit SlabAllocator(size_t slab_size_bytes = 2 * 1024 * 1024) 
        : slab_size_(slab_size_bytes), arena_(nullptr), offset_(0) {
        
        // Attempt allocation via huge pages, fallback to anonymous mapping if unavailable
        arena_ = mmap(nullptr, slab_size_, PROT_READ | PROT_WRITE, 
                      MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0);
        
        if (arena_ == MAP_FAILED) {
            arena_ = mmap(nullptr, slab_size_, PROT_READ | PROT_WRITE, 
                          MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            if (arena_ == MAP_FAILED) {
                throw TokenBucketException(ErrorCode::AllocationFailed, "Failed to allocate memory arena via mmap");
            }
        }

        // Pre-fault arena pages to guarantee deterministic O(1) allocation latencies
        char* ptr = static_cast<char*>(arena_);
        for (size_t i = 0; i < slab_size_; i += 4096) {
            ptr[i] = 0;
        }
    }

    ~SlabAllocator() {
        if (arena_ && arena_ != MAP_FAILED) {
            munmap(arena_, slab_size_);
        }
    }

    SlabAllocator(const SlabAllocator&) = delete;
    SlabAllocator& operator=(const SlabAllocator&) = delete;
    SlabAllocator(SlabAllocator&& other) noexcept 
        : slab_size_(other.slab_size_), arena_(other.arena_), offset_(other.offset_) {
        other.arena_ = nullptr;
        other.offset_ = 0;
    }

    SlabAllocator& operator=(SlabAllocator&& other) noexcept {
        if (this != &other) {
            if (arena_ && arena_ != MAP_FAILED) {
                munmap(arena_, slab_size_);
            }
            slab_size_ = other.slab_size_;
            arena_ = other.arena_;
            offset_ = other.offset_;
            other.arena_ = nullptr;
            other.offset_ = 0;
        }
        return *this;
    }

    template <typename T, typename... Args>
    T* allocate(Args&&... args) {
        size_t const alignment = alignof(T);
        size_t const size = sizeof(T);

        size_t current_offset = offset_;
        size_t padding = (alignment - (current_offset % alignment)) % alignment;
        size_t total_required = size + padding;

        if (current_offset + total_required > slab_size_) {
            throw TokenBucketException(ErrorCode::BufferFull, "Slab allocator exhausted");
        }

        offset_ = current_offset + total_required;
        char* raw_memory = static_cast<char*>(arena_) + current_offset + padding;
        
        return new (raw_memory) T(std::forward<Args>(args)...);
    }

    void reset() noexcept {
        offset_ = 0;
    }

    [[nodiscard]] size_t available_bytes() const noexcept {
        return slab_size_ - offset_;
    }

private:
    size_t slab_size_;
    void* arena_;
    size_t offset_;
};

// Lock-Free Multi-Producer Multi-Consumer (MPMC) Ring Buffer Node
template <typename T, size_t Capacity = 1024>
class LockFreeMPMCRingBuffer {
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of two");
public:
    LockFreeMPMCRingBuffer() noexcept {
        for (size_t i = 0; i < Capacity; ++i) {
            nodes_[i].sequence.store(i, std::memory_order_relaxed);
        }
        enqueue_pos_.store(0, std::memory_order_relaxed);
        dequeue_pos_.store(0, std::memory_order_relaxed);
    }

    ~LockFreeMPMCRingBuffer() = default;

    bool enqueue(const T& data) noexcept {
        Node* node;
        size_t pos = enqueue_pos_.load(std::memory_order_relaxed);
        
        for (;;) {
            node = &nodes_[pos & mask_];
            size_t seq = node->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos);

            if (dif == 0) {
                if (enqueue_pos_.compare_exchange_weak(pos, pos + 1, 
                                                      std::memory_order_relaxed, 
                                                      std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Buffer full
            } else {
                pos = enqueue_pos_.load(std::memory_order_relaxed);
            }
        }

        node->data = data;
        node->sequence.store(pos + 1, std::memory_order_release);
        return true;
    }

    bool dequeue(T& data) noexcept {
        Node* node;
        size_t pos = dequeue_pos_.load(std::memory_order_relaxed);

        for (;;) {
            node = &nodes_[pos & mask_];
            size_t seq = node->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(pos + 1);

            if (dif == 0) {
                if (dequeue_pos_.compare_exchange_weak(pos, pos + 1, 
                                                      std::memory_order_relaxed, 
                                                      std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false; // Buffer empty
            } else {
                pos = dequeue_pos_.load(std::memory_order_relaxed);
            }
        }

        data = node->data;
        node->sequence.store(pos + mask_ + 1, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        size_t dequeued = dequeue_pos_.load(std::memory_order_relaxed);
        size_t enqueued = enqueue_pos_.load(std::memory_order_relaxed);
        return dequeued == enqueued;
    }

private:
    struct Node {
        std::atomic<size_t> sequence;
        T data;
    };

    static constexpr size_t mask_ = Capacity - 1;
    std::array<Node, Capacity> nodes_;
    char pad1_[64]; // Padding to prevent false sharing between enqueue and dequeue pointers
    std::atomic<size_t> enqueue_pos_;
    char pad2_[64];
    std::atomic<size_t> dequeue_pos_;
};

// CAS Replenishment Loop with Exponential Backoff
inline bool replenish_tokens_cas(std::atomic<uint64_t>& token_storage, 
                                  uint64_t max_capacity, 
                                  uint64_t tokens_to_add) noexcept {
    uint64_t current = token_storage.load(std::memory_order_relaxed);
    uint32_t backoff_attempts = 0;

    for (;;) {
        uint64_t desired = (current + tokens_to_add > max_capacity) ? max_capacity : (current + tokens_to_add);
        if (token_storage.compare_exchange_weak(current, desired, 
                                                std::memory_order_release, 
                                                std::memory_order_relaxed)) {
            return true;
        }

        // Exponential backoff via pause instruction to mitigate bus contention
        for (uint32_t i = 0; i < (1U << (backoff_attempts & 7)); ++i) {
            _mm_pause();
        }
        if (backoff_attempts < 16) {
            backoff_attempts++;
        }
    }
}

} // namespace distributed_token_bucket

#endif // DISTRIBUTED_TOKEN_BUCKET_TYPES_HPP