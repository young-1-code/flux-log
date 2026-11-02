#pragma once
#include <memory>
#include <string>
#include <string_view>
namespace flux::detail {
struct LogItem;
class SymbolTable;
}  // namespace flux::detail
namespace flux {
class Formatter {
 public:
    virtual ~Formatter() = default;
    virtual void format(const detail::LogItem&, std::string&) noexcept = 0;
};
std::unique_ptr<Formatter> make_pattern_formatter(std::string_view pattern, detail::SymbolTable* names = nullptr,
                                                  detail::SymbolTable* files = nullptr);
}  // namespace flux
