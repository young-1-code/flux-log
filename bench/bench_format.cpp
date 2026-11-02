#include <chrono>
#include <format>
#include <iostream>
#include <string>

int main(int argc, char** argv)
{
    const std::size_t count = argc > 1 ? std::stoul(argv[1]) : 1000000;
    std::string output;
    output.reserve(256);
    const auto begin = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < count; ++i) {
        output.clear();
        std::format_to(std::back_inserter(output), "oid={} px={} side={}", i, 100.5, "buy");
    }
    const auto seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
    std::cout << "messages=" << count << " seconds=" << seconds
              << " formats_per_sec=" << static_cast<double>(count) / seconds << " last_size=" << output.size() << '\n';
}
