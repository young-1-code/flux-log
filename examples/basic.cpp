#include <chrono>
#include <flux/log.hpp>
#include <iostream>

int main()
{
    flux::Config config;
    config.queue_items = 16384;
    config.sinks = {flux::make_file_sink("flux-example.log"), flux::make_stdout_sink()};
    flux::init(std::move(config));

    auto& order = flux::get("order");
    auto& audit = flux::get("audit");
    order.set_level(flux::Level::Info);
    audit.set_overflow(flux::OverflowPolicy::Block);

    FLUX_DEBUG(order, "this is filtered by the default Info level");
    FLUX_INFO(order, "accepted order oid={} symbol={}", 42, "AAPL");
    FLUX_WARN(order, "order {} is close to its limit", 42);
    FLUX_ERROR(audit, "risk check failed for order {}", 42);

    flux::flush(std::chrono::seconds(1));
    const auto stats = flux::stats();
    std::cout << "accepted=" << stats.accepted << " queue_dropped=" << stats.queue_dropped << '\n';
    flux::shutdown(std::chrono::seconds(5));
}
