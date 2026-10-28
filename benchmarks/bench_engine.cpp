#include <iostream>
#include <chrono>
#include <vector>
#include <numeric>
#include <algorithm>
#include <thread>
#include <mutex>
#include <atomic>

// Implementation of a Distributed Token Bucket microbenchmark
class DistributedTokenBucket {
private:
    double capacity;
    double refill_rate; // tokens per second
    double tokens;
    std::chrono::steady_clock::time_point last_refill;
    std::mutex mtx;

    void refill() {
        auto now = std::chrono::steady_clock::now();
        std::chrono::duration<double> elapsed = now - last_refill;
        last_refill = now;
        
        tokens = std::min(capacity, tokens + elapsed.count() * refill_rate);
    }

public:
    DistributedTokenBucket(double cap, double rate) 
        : capacity(cap), refill_rate(rate), tokens(cap), last_refill(std::chrono::steady_clock::now()) {}

    bool consume(double tokens_requested) {
        std::lock_guard<std::mutex> lock(mtx);
        refill();
        if (tokens >= tokens_requested) {
            tokens -= tokens_requested;
            return true;
        }
        return false;
    }
};

int main() {
    const size_t iterations = 1000000;
    DistributedTokenBucket tb(1000000.0, 10000000.0);

    std::vector<double> latencies_ns;
    latencies_ns.reserve(iterations);

    // Warmup
    for (size_t i = 0; i < 10000; ++i) {
        tb.consume(1.0);
    }

    auto start_time = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < iterations; ++i) {
        auto op_start = std::chrono::high_resolution_clock::now();
        
        // Force spin/retry if capacity is momentarily hit, though rate is high here
        while (!tb.consume(1.0)) {
            // spin
        }

        auto op_end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(op_end - op_start).count();
        latencies_ns.push_back(static_cast<double>(duration));
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_total = end_time - start_time;

    // Calculate metrics
    double total_ops = static_cast<double>(iterations);
    double ops_per_sec = total_ops / elapsed_total.count();

    std::sort(latencies_ns.begin(), latencies_ns.end());

    double p50 = latencies_ns[iterations * 0.50];
    double p99 = latencies_ns[iterations * 0.99];
    double p999 = latencies_ns[iterations * 0.999];

    // Output clear formatted tabular metrics to stdout
    std::cout << "\n===================================================\n";
    std::cout << "       Distributed Token Bucket Microbenchmark     \n";
    std::cout << "===================================================\n";
    std::cout << "Metric              | Value                        \n";
    std::cout << "--------------------+------------------------------\n";
    std::cout << "Total Operations    | " << iterations << "\n";
    std::cout << "Duration (seconds)  | " << elapsed_total.count() << " s\n";
    std::cout << "Throughput          | " << ops_per_sec << " ops/sec\n";
    std::cout << "Latency p50         | " << p50 << " ns\n";
    std::cout << "Latency p99         | " << p99 << " ns\n";
    std::cout << "Latency p99.9       | " << p999 << " ns\n";
    std::cout << "===================================================\n";

    return 0;
}