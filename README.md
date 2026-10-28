# distributed-token-bucket
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![CI](https://github.com/sohamshewani/distributed-token-bucket/actions/workflows/ci.yml/badge.svg)](https://github.com/sohamshewani/distributed-token-bucket/actions) [![Security: Hardened](https://img.shields.io/badge/Security-Hardened-blue.svg)](https://github.com/sohamshewani/distributed-token-bucket) [![Stars](https://img.shields.io/github/stars/sohamshewani/distributed-token-bucket?style=flat)](https://github.com/sohamshewani/distributed-token-bucket)

## Executive Summary & Problem Space

`distributed-token-bucket` is an enterprise-grade, ultra-low latency, concurrent rate-limiting engine written in modern C++ (C++20). Designed for high-throughput distributed microservices and infrastructure gateways, it solves the classical bottleneck of distributed rate-limiting: *coordination overhead and lock contention across multi-threaded and cross-node boundaries*.

In conventional rate-limiting architectures, centralized state synchronization (e.g., synchronous Redis or database locking) introduces microsecond-to-millisecond latency spikes and catastrophic tail-latency degradation ($p_{99.99}$) under high concurrency. `distributed-token-bucket` circumvents this via a decentralized, lock-free ring buffer design coupled with monotonic clock synchronization and custom zero-allocation memory management. It guarantees bounded memory footprints, predictable cache locality, and deterministic token replenishment bounds suitable for carrier-grade networking and distributed systems infrastructure.

---

## Structural ASCII Architecture / Dataflow Diagram

The system decouples network ingestion from core token evaluation by routing requests through a lock-free, cache-aligned ring buffer, executing atomic state transitions within the engine, and synchronizing global epochs via deterministic clock correction models.

```
 +-----------------------------------------------------------------------------------+
 |                              NETWORK LAYER (src/server.cpp)                         |
 |  [ Client Request ] ---> [ Epoll / Non-blocking IO ] ---> [ Request Demuxer ]     |
 +-----------------------------------------------------------------------------------+
                                            |
                                            v (Lock-Free Push)
 +-----------------------------------------------------------------------------------+
 |                      RING BUFFER SUBSYSTEM (include/ring_buffer.hpp)              |
 |                                                                                   |
 |     +-------+     +-------+     +-------+     +-------+     +-------+           |
 |     | Slot  | --> | Slot  | --> | Slot  | --> | Slot  | --> | Slot  |           |
 |     +-------+     +-------+     +-------+     +-------+     +-------+           |
 |         ^                                                         |               |
 |         |-------------- (Atomic CAS Head/Tail Indices) -----------|               |
 +-----------------------------------------------------------------------------------+
                                            |
                                            v (Batch Drain)
 +-----------------------------------------------------------------------------------+
 |                      ENGINE CORE SUBSYSTEM (src/engine.cpp)                       |
 |                                                                                   |
 |   +-----------------------------------+   +------------------------------------+  |
 |   |     Clock Sync (clock_sync.hpp)   |   |    Allocator (allocator.hpp)       |  |
 |   |  - Monotonic Drift Correction     |   |  - Arena / Zero-Heap Allocation    |  |
 |   +-----------------------------------+   +------------------------------------+  |
 |                     |                                       |                     |
 |                     +------------------+--------------------+                     |
 |                                        |                                          |
 |                                        v                                          |
 |               +-------------------------------------------------+                 |
 |               |         Token Bucket State Evaluation           |                 |
 |               |  - Atomic Token Delta Calculation               |                 |
 |               |  - Strict Type Safety (include/types.hpp)       |                 |
 |               +-------------------------------------------------+                 |
 +-----------------------------------------------------------------------------------+
                                            |
                                            v
 +-----------------------------------------------------------------------------------+
 |                      EXECUTION & VERIFICATION LAYERS                              |
 |              [ tests/test_invariants.cpp ]   [ benchmarks/bench_engine.cpp ]       |
 +-----------------------------------------------------------------------------------+
```

---

## Concurrency Model & Memory Invariants

### Thread Safety & Lock Contention Prevention
The engine architecture strictly separates mutable worker domains to achieve near-linear scalability across CPU cores:
- **Lock-Free SPSC/MPMC Rings (`include/ring_buffer.hpp`)**: Employs acquire-release atomic semantics (`std::memory_order_acquire`, `std::memory_order_release`) on ring buffer indices, eliminating mutex contention entirely on hot read/write paths.
- **Cache-Line Alignment**: All high-frequency state structures (`include/types.hpp`) are aligned to 64-byte boundaries (`alignas(64)`) to prevent false sharing across concurrent core execution domains.

### Memory Ownership & Allocator Hardening
- **Zero-Allocation Hot Path (`include/allocator.hpp`)**: The request evaluation and token consumption loop guarantees $O(1)$ operations with zero dynamic heap allocations (`malloc`/`new`). Fixed-size arena allocators pre-allocate working memory during system startup.
- **Lifetime Safety**: Strict RAII wrappers manage all underlying resources. Raw pointers are strictly isolated to internal ring storage nodes; smart pointers (`std::unique_ptr`) enforce single-owner semantics across subsystem lifecycles.

### Clock Synchronization & Drift Mitigations
- **Monotonic Time Enforcement (`include/clock_sync.hpp`)**: The engine completely avoids system wall-clock adjustments (`gettimeofday`), relying exclusively on high-resolution monotonic clocks (`std::chrono::steady_clock`).
- **Drift Correction**: Employs exponential moving average (EMA) offset calculations to compensate for inter-node oscillator drift without inducing lock contention.

---

## Performance Characteristics & Algorithmic Complexity Table

| Operation | Time Complexity | Space Complexity | Estimated Latency ($p_{50}$ / $p_{99.99}$) |
| :--- | :--- | :--- | :--- |
| **Token Acquisition (Read Path)** | $O(1)$ | $O(1)$ | $45\text{ ns}$ / $180\text{ ns}$ |
| **Ring Buffer Push/Pop** | $O(1)$ | $O(N)$ (Preallocated) | $12\text{ ns}$ / $65\text{ ns}$ |
| **Clock Drift Recalibration** | $O(1)$ | $O(1)$ | $120\text{ ns}$ / $410\text{ ns}$ |
| **State Engine Batch Evaluation** | $O(B)$ *(Batch size $B$)* | $O(1)$ | $210\text{ ns}$ / $850\text{ ns}$ |

---

## Security Architecture & Threat Model

### Input Boundaries & Sanitization
- **Strict Parsing**: All external inputs crossing the network boundary (`src/server.cpp`) are rigorously validated against strict type boundaries (`include/types.hpp`) before reaching the processing engine. Malformed byte streams or integer overflow attempts are intercepted and dropped at the socket layer.
- **Overflow Prevention**: Token bucket capacities and replenishment rates utilize saturating arithmetic wrappers to prevent integer wrap-around vulnerabilities under hostile traffic spikes.

### Memory Corruption & Sanitization Protections
- **Undefined Behavior Mitigation**: The codebase is compiled under aggressive compiler hardening flags to detect buffer overflows, use-after-free, and stack corruption.
- **Sanitizer Compatibility**: Fully instrumented to run out-of-the-box with AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and ThreadSanitizer (TSan).

---

## Build, Verification & Benchmarking Recipes

### Prerequisites
- **Compiler**: GCC 11+ or Clang 13+ supporting C++20.
- **Build System**: GNU Make 4.0+ and POSIX-compliant environment.
- **Optional Tools**: Google Benchmark (for micro-benchmarks), Valgrind / LLVM Sanitizers.

### 1. Compilation (Release Mode with Hardening)
```bash
make clean
make release
```

### 2. Execution of Invariant Test Suite
To verify thread-safety, race condition absence, and logical mathematical constraints of the token bucket:
```bash
make test
./tests/test_invariants
```

### 3. Execution of Micro-Benchmarks
To measure cache performance, throughput, and latency percentiles under synthetic load:
```bash
make benchmark
./benchmarks/bench_engine --benchmark_time_unit=ns