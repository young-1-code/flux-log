#pragma once
#include <memory>
#include <string>
#include <string_view>
namespace flux::detail {
struct LogItem;
}  // namespace flux::detail
namespace flux {
// Formatter 只在后端线程运行，将 LogItem 组装为完整日志行。
class Formatter {
 public:
    virtual ~Formatter() = default;
    virtual void format(const detail::LogItem&, std::string&) noexcept = 0;
};
std::unique_ptr<Formatter> make_pattern_formatter(std::string_view pattern);
}  // namespace flux
