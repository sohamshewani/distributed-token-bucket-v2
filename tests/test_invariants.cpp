#include <iostream>
#include <chrono>
#include <thread>
#include <atomic>
#include <vector>
#include <mutex>
#include <memory>
#include <cassert>
#include <cmath>
#include <algorithm>
#include <random>

namespace DistributedTokenBucket {

class Clock {
public:
    virtual ~Clock() = default;
    virtual uint64_t now_ms() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()
        ).count();
    }
};

class MockClock : public Clock {
public:
    MockClock(uint64_t start_ms = 0) : current_time_ms_(start_ms) {}
    
    uint64_t now_ms() const override {
        return current_time_ms_.load(std::memory_order_acquire);
    }

    void advance(uint64_t ms) {
        current_time_ms_.fetch_add(ms, std::memory_order_release);
    }

    void set(uint64_t ms) {
        current_time_ms_.store(ms, std::memory_order_release);
    }

private:
    std::atomic<uint64_t> current_time_ms_;
};

struct TokenBucketConfig {
    double capacity;
    double refill_rate_per_sec;

    bool validate() const {
        return capacity > 0.0 && refill_rate_per_sec >= 0.0;
    }
};

class TokenBucketState {
public:
    double tokens;
    uint64_t last_refill_timestamp_ms;

    TokenBucketState() : tokens(0.0), last_refill_timestamp_ms(0) {}
    TokenBucketState(double t, uint64_t ts) : tokens(t), last_refill_timestamp_ms(ts) {}
};

class StorageBackend {
public:
    virtual ~StorageBackend() = default;
    virtual bool get(TokenBucketState& state) = 0;
    virtual bool compare_and_set(const TokenBucketState& expected, const TokenBucketState& desired) = 0;
};

class LocalAtomicStorage : public StorageBackend {
public:
    LocalAtomicStorage(double initial_tokens, uint64_t initial_timestamp)
        : state_(initial_tokens, initial_timestamp) {}

    bool get(TokenBucketState& state) override {
        std::lock_guard<std::mutex> lock(mutex_);
        state = state_;
        return true;
    }

    bool compare_and_set(const TokenBucketState& expected, const TokenBucketState& desired) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (std::abs(state_.tokens - expected.tokens) < 1e-9 &&
            state_.last_refill_timestamp_ms == expected.last_refill_timestamp_ms) {
            state_ = desired;
            return true;
        }
        return false;
    }

private:
    std::mutex mutex_;
    TokenBucketState state_;
};

class DistributedTokenBucket {
public:
    DistributedTokenBucket(TokenBucketConfig config, std::shared_ptr<StorageBackend> storage, std::shared_ptr<Clock> clock)
        : config_(config), storage_(storage), clock_(clock) {
        if (!config_.validate()) {
            throw std::invalid_argument("Invalid TokenBucketConfig");
        }
    }

    struct ConsumeResult {
        bool allowed;
        double remaining_tokens;
        uint64_t wait_time_ms;
    };

    ConsumeResult consume(double tokens_to_consume, int max_retries = 100) {
        if (tokens_to_consume <= 0.0) {
            TokenBucketState current;
            storage_->get(current);
            return {true, current.tokens, 0};
        }

        if (tokens_to_consume > config_.capacity) {
            TokenBucketState current;
            storage_->get(current);
            return {false, current.tokens, 0};
        }

        uint64_t now = clock_->now_ms();

        for (int i = 0; i < max_retries; ++i) {
            TokenBucketState current;
            if (!storage_->get(current)) {
                continue;
            }

            uint64_t elapsed_ms = (now > current.last_refill_timestamp_ms) ? (now - current.last_refill_timestamp_ms) : 0;
            double generated_tokens = (static_cast<double>(elapsed_ms) / 1000.0) * config_.refill_rate_per_sec;
            double current_tokens = std::min(config_.capacity, current.tokens + generated_tokens);
            uint64_t effective_timestamp = (elapsed_ms > 0) ? now : current.last_refill_timestamp_ms;

            if (current_tokens >= tokens_to_consume) {
                TokenBucketState desired(current_tokens - tokens_to_consume, effective_timestamp);
                if (storage_->compare_and_set(current, desired)) {
                    return {true, desired.tokens, 0};
                }
            } else {
                double deficit = tokens_to_consume - current_tokens;
                double seconds_to_wait = deficit / config_.refill_rate_per_sec;
                uint64_t wait_ms = static_cast<uint64_t>(std::ceil(seconds_to_wait * 1000.0));
                return {false, current_tokens, wait_ms};
            }
        }

        TokenBucketState current;
        storage_->get(current);
        return {false, current.tokens, 0};
    }

    TokenBucketConfig get_config() const { return config_; }

private:
    TokenBucketConfig config_;
    std::shared_ptr<StorageBackend> storage_;
    std::shared_ptr<Clock> clock_;
};

} // namespace DistributedTokenBucket

class FaultyStorageBackend : public DistributedTokenBucket::StorageBackend {
public:
    FaultyStorageBackend(std::shared_ptr<DistributedTokenBucket::StorageBackend> inner)
        : inner_(inner), fail_get_(false), fail_cas_(false) {}

    void set_fail_get(bool val) { fail_get_.store(val); }
    void set_fail_cas(bool val) { fail_cas_.store(val); }

    bool get(DistributedTokenBucket::TokenBucketState& state) override {
        if (fail_get_.load()) return false;
        return inner_->get(state);
    }

    bool compare_and_set(const DistributedTokenBucket::TokenBucketState& expected, 
                         const DistributedTokenBucket::TokenBucketState& desired) override {
        if (fail_cas_.load()) return false;
        return inner_->compare_and_set(expected, desired);
    }

private:
    std::shared_ptr<DistributedTokenBucket::StorageBackend> inner_;
    std::atomic<bool> fail_get_;
    std::atomic<bool> fail_cas_;
};

void run_unit_tests() {
    std::cout << "[RUNNING] Unit & Boundary Tests..." << std::endl;

    {
        auto clock = std::make_shared<DistributedTokenBucket::MockClock>(1000);
        auto storage = std::make_shared<DistributedTokenBucket::LocalAtomicStorage>(10.0, 1000);
        DistributedTokenBucket::TokenBucketConfig config{10.0, 5.0};
        DistributedTokenBucket::DistributedTokenBucket tb(config, storage, clock);

        auto res = tb.consume(5.0);
        assert(res.allowed == true);
        assert(std::abs(res.remaining_tokens - 5.0) < 1e-5);
        assert(res.wait_time_ms == 0);
    }

    {
        auto clock = std::make_shared<DistributedTokenBucket::MockClock>(1000);
        auto storage = std::make_shared<DistributedTokenBucket::LocalAtomicStorage>(2.0, 1000);
        DistributedTokenBucket::TokenBucketConfig config{10.0, 10.0};
        DistributedTokenBucket::DistributedTokenBucket tb(config, storage, clock);

        auto res = tb.consume(5.0);
        assert(res.allowed == false);
        assert(std::abs(res.remaining_tokens - 2.0) < 1e-5);
        assert(res.wait_time_ms == 300); 
    }

    {
        auto clock = std::make_shared<DistributedTokenBucket::MockClock>(1000);
        auto storage = std::make_shared<DistributedTokenBucket::LocalAtomicStorage>(5.0, 1000);
        DistributedTokenBucket::TokenBucketConfig config{10.0, 10.0};
        DistributedTokenBucket::DistributedTokenBucket tb(config, storage, clock);

        clock->advance(500); 
        auto res = tb.consume(8.0);
        assert(res.allowed == true);
        assert(std::abs(res.remaining_tokens - 2.0) < 1e-5);
    }

    {
        auto clock = std::make_shared<DistributedTokenBucket::MockClock>(1000);
        auto storage = std::make_shared<DistributedTokenBucket::LocalAtomicStorage>(0.0, 1000);
        DistributedTokenBucket::TokenBucketConfig config{10.0, 10.0};
        DistributedTokenBucket::DistributedTokenBucket tb(config, storage, clock);

        clock->advance(20000); 
        auto res = tb.consume(5.0);
        assert(res.allowed == true);
        
        DistributedTokenBucket::TokenBucketState state;
        storage->get(state);
        assert(state.tokens <= config.capacity);
    }

    {
        auto clock = std::make_shared<DistributedTokenBucket::MockClock>(1000);
        auto storage = std::make_shared<DistributedTokenBucket::LocalAtomicStorage>(5.0, 1000);
        DistributedTokenBucket::TokenBucketConfig config{10.0, 10.0};
        DistributedTokenBucket::DistributedTokenBucket tb(config, storage, clock);

        auto res_zero = tb.consume(0.0);
        assert(res_zero.allowed == true);
        assert(res_zero.wait_time_ms == 0);

        auto res_overflow = tb.consume(15.0);
        assert(res_overflow.allowed == false);
        assert(res_overflow.wait_time_ms == 0);
    }

    std::cout << "[PASSED] Unit & Boundary Tests." << std::endl;
}

void run_invariant_tests() {
    std::cout << "[RUNNING] Invariant & State Verification Tests..." << std::endl;

    auto clock = std::make_shared<DistributedTokenBucket::MockClock>(0);
    auto storage = std::make_shared<DistributedTokenBucket::LocalAtomicStorage>(50.0, 0);
    DistributedTokenBucket::TokenBucketConfig config{100.0, 20.0};
    DistributedTokenBucket::DistributedTokenBucket tb(config, storage, clock);

    std::mt19937 rng(1337);
    std::uniform_real_distribution<double> dist_consume(0.1, 10.0);
    std::uniform_int_distribution<uint64_t> dist_time(0, 500);

    for (int i = 0; i < 1000; ++i) {
        double consume_amount = dist_consume(rng);
        clock->advance(dist_time(rng));

        auto res = tb.consume(consume_amount);

        DistributedTokenBucket::TokenBucketState state;
        storage->get(state);

        assert(state.tokens >= 0.0);
        assert(state.tokens <= config.capacity);

        if (res.allowed) {
            assert(state.tokens <= config.capacity);
        }
    }

    std::cout << "[PASSED] Invariant & State Verification Tests." << std::endl;
}

void run_concurrent_stress_tests() {
    std::cout << "[RUNNING] Concurrent Stress & Race Condition Tests..." << std::endl;

    const int num_threads = 32;
    const int iterations_per_thread = 5