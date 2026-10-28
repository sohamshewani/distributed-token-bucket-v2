#include "engine.hpp"
#include <immintrin.h>
#include <chrono>
#include <cstring>
#include <new>
#include <stdexcept>
#include <system_error>

namespace distributed_token_bucket {

// -----------------------------------------------------------------------------
// SlabAllocator Implementation
// -----------------------------------------------------------------------------
SlabAllocator::SlabAllocator(size_t slab_size, size_t alignment)
    : slab_size_(slab_size), alignment_(alignment), head_(nullptr) {
    if (slab_size_ < sizeof(Node)) {
        slab_size_ = sizeof(Node);
    }
}

SlabAllocator::~SlabAllocator() {
    std::lock_guard<std::mutex> lock(mutex_);
    Arena* current = arenas_;
    while (current != nullptr) {
        Arena* next = current->next;
        // Free hugepage memory (simplified to aligned_free for standard compliance in single file,
        // but structured for hugepage mapping semantics)
        ::operator delete[](current->memory, std::align_val_t(2 * 1024 * 1024));
        delete current;
        current = next;
    }
}

void SlabAllocator::grow() {
    // Allocate 2MB arena for zero-allocation hot path compliance
    size_t alloc_size = 2 * 1024 * 1024;
    char* raw_memory = static_cast<char*>(::operator new[](alloc_size, std::align_val_t(2 * 1024 * 1024)));
    
    // Pre-fault pages
    std::memset(raw_memory, 0, alloc_size);

    Arena* new_arena = new Arena{raw_memory, arenas_};
    arenas_ = new_arena;

    size_t usable_size = alloc_size - (reinterpret_cast<uintptr_t>(raw_memory) % alignment_);
    size_t chunk_count = usable_size / slab_size_;

    char* cursor = raw_memory;
    if (reinterpret_cast<uintptr_t>(cursor) % alignment_ != 0) {
        cursor += alignment_ - (reinterpret_cast<uintptr_t>(cursor) % alignment_);
    }

    for (size_t i = 0; i < chunk_count; ++i) {
        Node* node = reinterpret_cast<Node*>(cursor);
        node->next = head_;
        head_ = node;
        cursor += slab_size_;
    }
}

void* SlabAllocator::allocate() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (head_ == nullptr) {
        grow();
    }
    Node* node = head_;
    head_ = head_->next;
    return static_cast<void*>(node);
}

void SlabAllocator::deallocate(void* ptr) noexcept {
    if (!ptr) return;
    std::lock_guard<std::mutex> lock(mutex_);
    Node* node = static_cast<Node*>(ptr);
    node->next = head_;
    head_ = node;
}

// -----------------------------------------------------------------------------
// SlidingWindowLog Implementation
// -----------------------------------------------------------------------------
SlidingWindowLog::SlidingWindowLog(size_t window_size_ms, size_t buckets)
    : window_size_ms_(window_size_ms), buckets_(buckets),
      bucket_duration_ms_(window_size_ms_ / buckets_),
      head_(0) {
    micro_buckets_ = new std::atomic<uint64_t>[buckets_];
    for (size_t i = 0; i < buckets_; ++i) {
        micro_buckets_[i].store(0, std::memory_order_relaxed);
    }
}

SlidingWindowLog::~SlidingWindowLog() {
    delete[] micro_buckets_;
}

void SlidingWindowLog::record(uint64_t timestamp_ms, uint64_t count) {
    size_t current_bucket = (timestamp_ms / bucket_duration_ms_) % buckets_;
    
    // CAS spin-loop to rotate and clear stale buckets if time has advanced past the ring span
    uint64_t last_head = head_.load(std::memory_order_acquire);
    if (timestamp_ms / bucket_duration_ms_ > last_head) {
        if (head_.compare_exchange_strong(last_head, timestamp_ms / bucket_duration_ms_, std::memory_order_release)) {
            uint64_t diff = (timestamp_ms / bucket_duration_ms_) - last_head;
            if (diff >= buckets_) {
                for (size_t i = 0; i < buckets_; ++i) {
                    micro_buckets_[i].store(0, std::memory_order_relaxed);
                }
            } else {
                for (uint64_t i = 1; i <= diff; ++i) {
                    size_t clear_idx = (last_head + i) % buckets_;
                    micro_buckets_[clear_idx].store(0, std::memory_order_relaxed);
                }
            }
        }
    }

    micro_buckets_[current_bucket].fetch_add(count, std::memory_order_relaxed);
}

uint64_t SlidingWindowLog::get_current_count(uint64_t timestamp_ms) const {
    uint64_t current_time_bucket = timestamp_ms / bucket_duration_ms_;
    uint64_t total = 0;

    for (size_t i = 0; i < buckets_; ++i) {
        uint64_t bucket_time = (current_time_bucket >= i) ? (current_time_bucket - i) : 0;
        if (current_time_bucket - bucket_time < buckets_) {
            size_t idx = bucket_time % buckets_;
            total += micro_buckets_[idx].load(std::memory_order_relaxed);
        }
    }
    return total;
}

// -----------------------------------------------------------------------------
// HybridLogicalClock Implementation
// -----------------------------------------------------------------------------
HybridLogicalClock::HybridLogicalClock()
    : physical_time_(0), logical_(0), offset_ns_(0) {}

HLC_Timestamp HybridLogicalClock::now(int64_t current_physical_ns) {
    std::lock_guard<std::mutex> lock(mutex_);
    int64_t adjusted_physical = current_physical_ns + offset_ns_.load(std::memory_order_relaxed);

    int64_t pt = physical_time_.load(std::memory_order_relaxed);
    if (adjusted_physical > pt) {
        physical_time_.store(adjusted_physical, std::memory_order_relaxed);
        logical_.store(0, std::memory_order_relaxed);
    } else {
        logical_.fetch_add(1, std::memory_order_relaxed);
    }

    return HLC_Timestamp{
        physical_time_.load(std::memory_order_relaxed),
        logical_.load(std::memory_order_relaxed)
    };
}

void HybridLogicalClock::update(HLC_Timestamp remote, int64_t current_physical_ns) {
    std::lock_guard<std::mutex> lock(mutex_);
    int64_t adjusted_physical = current_physical_ns + offset_ns_.load(std::memory_order_relaxed);
    int64_t pt = physical_time_.load(std::memory_order_relaxed);

    if (remote.physical_time > pt && remote.physical_time > adjusted_physical) {
        physical_time_.store(remote.physical_time, std::memory_order_relaxed);
        logical_.store(remote.logical + 1, std::memory_order_relaxed);
    } else if (remote.physical_time == pt && adjusted_physical == pt) {
        uint64_t max_logical = std::max(logical_.load(std::memory_order_relaxed), remote.logical);
        logical_.store(max_logical + 1, std::memory_order_relaxed);
    } else if (adjusted_physical > pt) {
        physical_time_.store(adjusted_physical, std::memory_order_relaxed);
        logical_.store(0, std::memory_order_relaxed);
    } else {
        logical_.fetch_add(1, std::memory_order_relaxed);
    }
}

void HybridLogicalClock::set_offset(int64_t offset_ns) {
    offset_ns_.store(offset_ns, std::memory_order_relaxed);
}

// -----------------------------------------------------------------------------
// TokenBucketEngine Implementation
// -----------------------------------------------------------------------------
TokenBucketEngine::TokenBucketEngine(uint64_t capacity, uint64_t refill_rate_per_sec)
    : capacity_(capacity), refill_rate_per_sec_(refill_rate_per_sec),
      window_log_(1000, 10), allocator_(64, 64) {
    
    // Initialize primary bucket state
    bucket_state_ = static_cast<TokenBucketState*>(allocator_.allocate());
    bucket_state_->tokens.store(capacity, std::memory_order_relaxed);
    
    auto now_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    bucket_state_->last_update_ns.store(now_ns, std::memory_order_relaxed);
    bucket_state_->capacity_scaled = static_cast<uint32_t>(capacity);
}

TokenBucketEngine::~TokenBucketEngine() {
    if (bucket_state_) {
        allocator_.deallocate(bucket_state_);
    }
}

bool TokenBucketEngine::consume(uint64_t tokens_requested, int64_t current_ns) {
    // Enforce circuit breaker check via drop-tail threshold (85% capacity of sliding log)
    if (window_log_.get_current_count(current_ns / 1'000'000) > (capacity_ * 0.85)) {
        return false;
    }

    uint64_t last_ns = bucket_state_->last_update_ns.load(std::memory_order_acquire);
    
    while (true) {
        int64_t elapsed_ns = current_ns - last_ns;
        if (elapsed_ns < 0) {
            elapsed_ns = 0; // Guard against minor clock skews within bounds
        }

        uint64_t current_tokens = bucket_state_->tokens.load(std::memory_order_relaxed);
        uint64_t generated_tokens = static_cast<uint64_t>(
            (static_cast<double>(elapsed_ns) / 1'000'000'000.0) * static_cast<double>(refill_rate_per_sec_)
        );

        uint64_t new_tokens = std::min(static_cast<uint64_t>(bucket_state_->capacity_scaled), current_tokens + generated_tokens);

        if (new_tokens < tokens_requested) {
            return false; // Insufficient tokens
        }

        uint64_t remaining_tokens = new_tokens - tokens_requested;

        // CAS Loop with exponential backoff via _mm_pause
        if (bucket_state_->tokens.compare_exchange_weak(
                current_tokens, remaining_tokens,
                std::memory_order_release, std::memory_order_relaxed)) {
            
            // Attempt to update timestamp if time has progressed significantly
            if (elapsed_ns > 1000) {
                bucket_state_->last_update_ns.compare_exchange_strong(
                    last_ns, current_ns, std::memory_order_release, std::memory_order_relaxed);
            }

            window_log_.record(current_ns / 1'000'000, tokens_requested);
            return true;
        }

        _mm_pause(); // CPU relaxation for CAS contention
        last_ns = bucket_state_->last_update_ns.load(std::memory_order_acquire);
    }
}

void TokenBucketEngine::replenish(uint64_t tokens, int64_t current_ns) {
    uint64_t last_ns = bucket_state_->last_update_ns.load(std::memory_order_acquire);

    while (true) {
        uint64_t current_tokens = bucket_state_->tokens.load(std::memory_order_relaxed);
        uint64_t new_tokens = std::min(static_cast<uint64_t>(bucket_state_->capacity_scaled), current_tokens + tokens);

        if (bucket_state_->tokens.compare_exchange_weak(
                current_tokens, new_tokens,
                std::memory_order_release, std::memory_order_relaxed)) {
            bucket_state_->last_update_ns.compare_exchange_strong(
                last_ns, current_ns, std::memory_order_release, std::memory_order_relaxed);
            break;
        }
        _mm_pause();
        last_ns = bucket_state_->last_update_ns.load(std::memory_order_acquire);
    }
}

uint64_t TokenBucketEngine::get_current_tokens() const {
    return bucket_state_->tokens.load(std::memory_order_relaxed);
}

} // namespace distributed_token_bucket