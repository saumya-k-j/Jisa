// Phase-1 microbenchmarks: per-op latency percentiles (p50/p99/p99.9) and
// throughput for the hot-path structures. Per cpp-conventions each hot-path
// structure reports latency percentiles; we time each operation with
// steady_clock and compute percentiles from the sorted sample vector
// (see DECISIONS.md for why not Google Benchmark + HdrHistogram).
//
// Run: ./build/benchmarks/bench_core   (Release build; Debug numbers are
// meaningless and the harness warns if NDEBUG is not set).

#include <core/message.hpp>
#include <core/pool_allocator.hpp>
#include <core/ring_buffer.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

struct Percentiles {
    double p50, p99, p999;
};

Percentiles percentiles(std::vector<double>& ns) {
    std::sort(ns.begin(), ns.end());
    auto at = [&](double q) {
        std::size_t i = static_cast<std::size_t>(q * static_cast<double>(ns.size() - 1));
        return ns[i];
    };
    return {at(0.50), at(0.99), at(0.999)};
}

void report(const char* name, std::vector<double>& samples) {
    const Percentiles p = percentiles(samples);
    std::printf("%-32s  n=%-9zu p50=%8.1f ns  p99=%8.1f ns  p99.9=%8.1f ns\n",
                name, samples.size(), p.p50, p.p99, p.p999);
}

telemetry::Message make_msg(std::uint64_t i) {
    telemetry::Message m{};
    m.stream_id = 1;
    m.ts_ns = static_cast<std::int64_t>(i);
    m.value = static_cast<double>(i) * 0.5;
    m.seq = i;
    return m;
}

constexpr std::size_t kSamples = 1'000'000;
constexpr std::size_t kCap = 4096;

// SPSC: single-thread push+pop pairs (uncontended per-op cost).
void bench_spsc_single() {
    static telemetry::SpscRingBuffer<telemetry::Message, kCap> rb;
    std::vector<double> push_ns, pop_ns;
    push_ns.reserve(kSamples);
    pop_ns.reserve(kSamples);
    telemetry::Message out{};
    for (std::uint64_t i = 0; i < kSamples; ++i) {
        const telemetry::Message m = make_msg(i);
        auto t0 = Clock::now();
        (void)rb.try_push(m);
        auto t1 = Clock::now();
        (void)rb.try_pop(out);
        auto t2 = Clock::now();
        push_ns.push_back(std::chrono::duration<double, std::nano>(t1 - t0).count());
        pop_ns.push_back(std::chrono::duration<double, std::nano>(t2 - t1).count());
    }
    report("spsc.try_push (uncontended)", push_ns);
    report("spsc.try_pop  (uncontended)", pop_ns);
}

// SPSC: two-thread throughput (msgs/sec end to end).
void bench_spsc_throughput() {
    static telemetry::SpscRingBuffer<telemetry::Message, kCap> rb;
    constexpr std::uint64_t kN = 10'000'000;
    std::atomic<bool> go{false};

    std::thread producer([&] {
        while (!go.load(std::memory_order_acquire)) {}
        for (std::uint64_t i = 0; i < kN; ++i) {
            const telemetry::Message m = make_msg(i);
            while (!rb.try_push(m)) {}
        }
    });

    std::uint64_t received = 0;
    telemetry::Message out{};
    const auto t0 = Clock::now();
    go.store(true, std::memory_order_release);
    while (received < kN) {
        if (rb.try_pop(out)) ++received;
    }
    const auto t1 = Clock::now();
    producer.join();

    const double secs = std::chrono::duration<double>(t1 - t0).count();
    std::printf("%-32s  %zu msgs in %.3f s  =  %.2fM msgs/sec\n",
                "spsc two-thread throughput", static_cast<std::size_t>(kN), secs,
                static_cast<double>(kN) / secs / 1e6);
}

// MPSC: per-op push cost with 4 producers (contended), consumer drains.
void bench_mpsc_contended() {
    static telemetry::MpscRingBuffer<telemetry::Message, kCap> rb;
    constexpr int kProducers = 4;
    constexpr std::uint64_t kPerProducer = 250'000;

    std::vector<std::vector<double>> samples(kProducers);
    std::atomic<bool> go{false};
    std::vector<std::thread> producers;
    producers.reserve(kProducers);
    for (int p = 0; p < kProducers; ++p) {
        producers.emplace_back([&, p] {
            samples[static_cast<std::size_t>(p)].reserve(kPerProducer);
            while (!go.load(std::memory_order_acquire)) {}
            for (std::uint64_t i = 0; i < kPerProducer; ++i) {
                const telemetry::Message m = make_msg(i);
                auto t0 = Clock::now();
                while (!rb.try_push(m)) { std::this_thread::yield(); }
                auto t1 = Clock::now();
                samples[static_cast<std::size_t>(p)].push_back(
                    std::chrono::duration<double, std::nano>(t1 - t0).count());
            }
        });
    }

    std::uint64_t received = 0;
    telemetry::Message out{};
    go.store(true, std::memory_order_release);
    while (received < kProducers * kPerProducer) {
        if (rb.try_pop(out)) ++received;
    }
    for (auto& t : producers) t.join();

    std::vector<double> all;
    all.reserve(kProducers * kPerProducer);
    for (auto& s : samples) all.insert(all.end(), s.begin(), s.end());
    report("mpsc.try_push (4 producers)", all);
}

// Pool allocator: allocate/deallocate pair cost.
void bench_pool() {
    telemetry::PoolAllocator pool(sizeof(telemetry::Message), 1024);
    std::vector<double> alloc_ns, dealloc_ns;
    alloc_ns.reserve(kSamples);
    dealloc_ns.reserve(kSamples);
    for (std::size_t i = 0; i < kSamples; ++i) {
        auto t0 = Clock::now();
        void* p = pool.allocate();
        auto t1 = Clock::now();
        pool.deallocate(p);
        auto t2 = Clock::now();
        alloc_ns.push_back(std::chrono::duration<double, std::nano>(t1 - t0).count());
        dealloc_ns.push_back(std::chrono::duration<double, std::nano>(t2 - t1).count());
    }
    report("pool.allocate", alloc_ns);
    report("pool.deallocate", dealloc_ns);
}

}  // namespace

int main() {
#ifndef NDEBUG
    std::printf("WARNING: non-Release build; numbers are not meaningful.\n");
#endif
    std::printf("bench_core — timer overhead included in every sample "
                "(steady_clock, ~20-40 ns on this class of hardware)\n");
    bench_spsc_single();
    bench_spsc_throughput();
    bench_mpsc_contended();
    bench_pool();
    return 0;
}
