#pragma once
#include <chrono>
#include <cstdio>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include "config.hpp"
namespace flux {
// Sink 由唯一后端线程调用；实现必须把 I/O 异常转换为 false。
class Sink {
 public:
    virtual ~Sink() = default;
    virtual bool write(std::span<const std::string_view>) noexcept = 0;
    virtual bool flush() noexcept = 0;
    // fsync 策略属于 sink，而 flush_interval 属于全局后端。
    virtual void set_fsync(FsyncPolicy p) noexcept { fsync_ = p; }
    FsyncPolicy fsync_policy() const noexcept { return fsync_; }
    void set_level(Level l) noexcept { level_ = l; }
    Level level() const noexcept { return level_; }
    virtual std::string name() const { return {}; }

 protected:
    FsyncPolicy fsync_{FsyncPolicy::OnRotation};
    Level level_{Level::Trace};
};
std::shared_ptr<Sink> make_file_sink(std::string path, std::optional<Rotate> rotate = std::nullopt,
                                     std::size_t buffer_size = 1 << 20);
std::shared_ptr<Sink> make_stdout_sink(std::FILE* out = stdout);
std::shared_ptr<Sink> make_rotating_file_sink(std::string path, Rotate rotate, std::size_t buffer_size = 1 << 20);
using DailyClock = std::function<std::chrono::system_clock::time_point()>;
std::shared_ptr<Sink> make_daily_file_sink(std::string path, unsigned keep, DailyClock clock = {});
}  // namespace flux
