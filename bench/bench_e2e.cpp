#include <chrono>
#include <filesystem>
#include <flux/log.hpp>
#include <iostream>

int main(int argc, char** argv)
{
    const std::size_t count = argc > 1 ? std::stoul(argv[1]) : 100000;
    const std::string path = argc > 2 ? argv[2] : "/tmp/flux-log-bench.log";
    std::filesystem::remove(path);
    flux::Config cfg;
    cfg.queue_items = 1u << 16;
    cfg.sinks = {flux::make_file_sink(path, std::nullopt, 1u << 20)};
    flux::init(std::move(cfg));
    auto& logger = flux::get("bench-e2e");
    const auto begin = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < count; ++i)
        FLUX_INFO(logger, "sequence={} payload=flux-log", i);
    flux::shutdown(std::chrono::seconds(10));
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
    const auto stats = flux::stats();
    std::cout << "messages=" << count << " seconds=" << elapsed
              << " msg_per_sec=" << static_cast<double>(count) / elapsed << " accepted=" << stats.accepted
              << " queue_dropped=" << stats.queue_dropped << '\n';
}
