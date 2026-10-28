#ifndef DISTRIBUTED_TOKEN_BUCKET_RING_BUFFER_HPP
#define DISTRIBUTED_TOKEN_BUCKET_RING_BUFFER_HPP

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <new>
#include <immintrin.h>
#include <stdexcept>
#include <system_error>

namespace distributed_token_bucket {

template <typename T, size_t Capacity>
class alignas(64) RingBuffer {
public:
    static_assert((Capacity & (Capacity - 1)) == 0, "RingBuffer capacity must be a power of two.");

    RingBuffer() noexcept 
        : head_(0), tail_(0) {
        for (size_t i = 0; i < Capacity; ++i) {
            slots_[i].sequence.store(i, std::memory_order_relaxed);
        }
    }

    ~RingBuffer() noexcept {
        T item;
        while (dequeue(item)) {
            // Drain remaining items to ensure proper cleanup if T requires it
        }
    }

    RingBuffer(const RingBuffer&) = delete;
    RingBuffer& operator=(const RingBuffer&) = delete;
    RingBuffer(RingBuffer&&) = delete;
    RingBuffer& operator=(RingBuffer&&) = delete;

    bool enqueue(const T& item) noexcept {
        Slot* slot;
        size_t head = head_.load(std::memory_order_relaxed);
        for (;;) {
            slot = &slots_[head & kIndexMask];
            size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(head);
            
            if (dif == 0) {
                if (head_.compare_exchange_weak(head, head + 1, 
                                                std::memory_order_relaxed, 
                                                std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                // Queue is full or head wrapped incorrectly relative to sequence
                return false;
            } else {
                head = head_.load(std::memory_order_relaxed);
            }
        }

        new (&slot->storage) T(item);
        slot->sequence.store(head + 1, std::memory_order_release);
        return true;
    }

    bool enqueue(T&& item) noexcept {
        Slot* slot;
        size_t head = head_.load(std::memory_order_relaxed);
        for (;;) {
            slot = &slots_[head & kIndexMask];
            size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(head);
            
            if (dif == 0) {
                if (head_.compare_exchange_weak(head, head + 1, 
                                                std::memory_order_relaxed, 
                                                std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                return false;
            } else {
                head = head_.load(std::memory_order_relaxed);
            }
        }

        new (&slot->storage) T(std::move(item));
        slot->sequence.store(head + 1, std::memory_order_release);
        return true;
    }

    bool dequeue(T& item) noexcept {
        Slot* slot;
        size_t tail = tail_.load(std::memory_order_relaxed);
        for (;;) {
            slot = &slots_[tail & kIndexMask];
            size_t seq = slot->sequence.load(std::memory_order_acquire);
            intptr_t dif = static_cast<intptr_t>(seq) - static_cast<intptr_t>(tail + 1);
            
            if (dif == 0) {
                if (tail_.compare_exchange_weak(tail, tail + 1, 
                                                std::memory_order_relaxed, 
                                                std::memory_order_relaxed)) {
                    break;
                }
            } else if (dif < 0) {
                // Queue is empty
                return false;
            } else {
                tail = tail_.load(std::memory_order_relaxed);
            }
        }

        item = std::move(*reinterpret_cast<T*>(&slot->storage));
        reinterpret_cast<T*>(&slot->storage)->~T();
        slot->sequence.store(tail + Capacity, std::memory_order_release);
        return true;
    }

    [[nodiscard]] bool empty() const noexcept {
        intptr_t head = static_cast<intptr_t>(head_.load(std::memory_order_relaxed));
        intptr_t tail = static_cast<intptr_t>(tail_.load(std::memory_order_relaxed));
        return (tail - head) >= 0;
    }

    [[nodiscard]] size_t size() const noexcept {
        size_t head = head_.load(std::memory_order_relaxed);
        size_t tail = tail_.load(std::memory_order_relaxed);
        if (head >= tail) {
            return head - tail;
        }
        return 0;
    }

    [[nodiscard]] constexpr size_t capacity() const noexcept {
        return Capacity;
    }

    [[nodiscard]] bool is_pressure_high(double threshold_ratio = 0.85) const noexcept {
        size_t current_size = size();
        size_t limit = static_cast<size_t>(static_cast<double>(Capacity) * threshold_ratio);
        return current_size >= limit;
    }

private:
    struct Slot {
        std::atomic<size_t> sequence;
        alignas(T) char storage[sizeof(T)];
    };

    static constexpr size_t kIndexMask = Capacity - 1;

    alignas(64) std::atomic<size_t> head_;
    alignas(64) std::atomic<size_t> tail_;
    Slot slots_[Capacity];
};

} // namespace distributed_token_bucket

#endif // DISTRIBUTED_TOKEN_BUCKET_RING_BUFFER_HPP