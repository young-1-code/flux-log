#pragma once
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include "level.hpp"
namespace flux {
class Sink;
enum class OverflowPolicy : std::uint8_t { DropNewest, Block };
enum class FsyncPolicy : std::uint8_t { Never, OnRotation, Always };
struct Rotate {
    std::size_t max_size{};
    unsigned keep{};
};
struct Stats {
    std::uint64_t accepted{}, queue_dropped{}, shutdown_abandoned{}, format_failed{}, oversize_truncated{};
    std::uint64_t sink_filtered{}, sink_failed{}, pre_init{}, post_shutdown{}, flushes{};
    std::unordered_map<std::string, std::uint64_t> persisted;
};
struct Config {
    std::size_t queue_items = 16384;
    OverflowPolicy overflow = OverflowPolicy::DropNewest;
    std::chrono::milliseconds flush_interval{200};
    bool pin_backend = false;
    std::vector<std::shared_ptr<Sink>> sinks;
    std::string pattern;
};
}  // namespace flux
