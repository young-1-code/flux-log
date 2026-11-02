#include <chrono>
#include <flux/log.hpp>
#include <iostream>
#include <thread>
#include <vector>

int main(int argc, char** argv)
{
    const std::size_t threads = argc > 1 ? std::stoul(argv[1]) : 1;
    const std::size_t per_thread = argc > 2 ? std::stoul(argv[2]) : 100000;
    flux::Config cfg;
    cfg.queue_items = 1u << 20;
    cfg.sinks = {flux::make_stdout_sink()};
    auto sink = cfg.sinks.front();
    sink->set_level(flux::Level::Off);
    flux::init(std::move(cfg));
    auto& logger = flux::get("bench");
    const auto begin = std::chrono::steady_clock::now();
    std::vector<std::thread> workers;
    workers.reserve(threads);
    for (std::size_t t = 0; t < threads; ++t) {
        workers.emplace_back([&, t] {
            for (std::size_t i = 0; i < per_thread; ++i)
                FLUX_INFO(logger, "thread={} sequence={}", t, i);
        });
    }
    for (auto& worker : workers)
        worker.join();
    flux::shutdown(std::chrono::seconds(10));
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
    const auto total = static_cast<double>(threads * per_thread);
    std::cout << "messages=" << static_cast<std::uint64_t>(total) << " seconds=" << elapsed
              << " msg_per_sec=" << total / elapsed << '\n';
}
