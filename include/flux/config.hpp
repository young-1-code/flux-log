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
// 队列满时的业务线程行为：丢弃新消息，或等待空间。
enum class OverflowPolicy : std::uint8_t { DropNewest, Block };
enum class FsyncPolicy : std::uint8_t { Never, OnRotation, Always };
struct Rotate {
    std::size_t max_size{};
    unsigned keep{};
};
struct Stats {
    // accepted 是成功入队的消息数；其余字段按消息离开链路的原因分类。
    std::uint64_t accepted{}, queue_dropped{}, shutdown_abandoned{}, format_failed{}, oversize_truncated{};
    std::uint64_t metadata_truncated{};
    std::uint64_t sink_filtered{}, sink_failed{}, pre_init{}, post_shutdown{}, flushes{};
    std::unordered_map<std::string, std::uint64_t> persisted;
};
struct Config {
    // 配置在 init 时校验；运行期间 sink 集合不变。
    std::size_t queue_items = 16384;
    OverflowPolicy overflow = OverflowPolicy::DropNewest;
    std::chrono::milliseconds flush_interval{200};
    bool pin_backend = false;
    std::vector<std::shared_ptr<Sink>> sinks;
    std::string pattern;
};
}  // namespace flux
