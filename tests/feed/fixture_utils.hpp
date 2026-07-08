// tests/feed/fixture_utils.hpp
//
// Test-only helper (NOT implementation code): loads lines from the recorded
// Coinbase fixture so multiple test binaries can share one small reader
// instead of duplicating file I/O. FIXTURE_DIR is injected as a compile
// definition by tests/CMakeLists.txt (points at tests/feed/fixtures).
#pragma once

#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#ifndef FIXTURE_DIR
#error "FIXTURE_DIR must be defined by the build (see tests/CMakeLists.txt)"
#endif

namespace telemetry::test {

inline std::string FixturePath(const std::string& filename) {
    return std::string(FIXTURE_DIR) + "/" + filename;
}

// Reads the fixture file and returns one string per non-empty line, in file
// order (1-based line numbers referenced in test comments == index + 1).
inline std::vector<std::string> LoadFixtureLines(const std::string& filename) {
    std::ifstream in(FixturePath(filename));
    if (!in) {
        throw std::runtime_error("could not open fixture: " + FixturePath(filename));
    }
    std::vector<std::string> lines;
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) {
            lines.push_back(line);
        }
    }
    return lines;
}

inline constexpr const char* kCoinbaseFixture = "coinbase_ticker_raw.jsonl";

} // namespace telemetry::test
