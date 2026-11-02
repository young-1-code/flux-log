#include <chrono>
#include <flux/logger.hpp>
#include <iostream>

int main(int argc, char** argv)
{
    const std::size_t count = argc > 1 ? std::stoul(argv[1]) : 10000000;
    flux::Logger logger;
    const auto begin = std::chrono::steady_clock::now();
    std::size_t passed = 0;
    for (std::size_t i = 0; i < count; ++i)
        if (logger.should_log(flux::Level::Info)) ++passed;
    const auto seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - begin).count();
    std::cout << "checks=" << count << " passed=" << passed << " seconds=" << seconds
              << " checks_per_sec=" << static_cast<double>(count) / seconds << '\n';
}
