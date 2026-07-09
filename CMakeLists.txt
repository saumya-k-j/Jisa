cmake_minimum_required(VERSION 3.24)
project(telemetry_engine CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Warnings apply to project targets only (not third-party like googletest,
# whose headers/sources are not -Werror-clean under newer clang).
add_library(telemetry_warnings INTERFACE)
target_compile_options(telemetry_warnings INTERFACE -Wall -Wextra -Werror)

# Sanitizers: SANITIZE=address,undefined (default for Debug) or thread or off.
# ASan/UBSan and TSan are mutually exclusive, hence separate build trees.
set(SANITIZE "off" CACHE STRING "Sanitizer set: off | address,undefined | thread")
if(NOT SANITIZE STREQUAL "off")
  add_compile_options(-fsanitize=${SANITIZE} -fno-omit-frame-pointer -g)
  add_link_options(-fsanitize=${SANITIZE})
endif()

# Core hot-path library: header-only (templates, fixed-capacity structures).
# Dependency-free by design: it must not pull in simdjson or any feed code.
add_library(telemetry_core INTERFACE)
target_include_directories(telemetry_core INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)

enable_testing()

include(FetchContent)

# ON by default (unchanged local/CI behavior); the Docker deployment build
# turns them OFF so the runtime image build does not fetch googletest or
# compile the 180-test suite (tests run natively/in CI, not per-image-build).
option(BUILD_TESTS "Build the C++ test suite" ON)
option(BUILD_BENCHMARKS "Build the microbenchmarks" ON)

if(BUILD_TESTS)
  # GoogleTest built from source so sanitizer instrumentation matches the tests.
  FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    SYSTEM  # -isystem: suppress warnings from gtest headers in our TUs
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)
endif()

# simdjson for the feed/parse path (SPEC-3.4). SYSTEM so its headers are
# included with -isystem and our -Wall -Wextra -Werror does not apply to them
# (mirrors the googletest entry). Pinned release tag for reproducibility.
FetchContent_Declare(
  simdjson
  URL https://github.com/simdjson/simdjson/archive/refs/tags/v3.10.1.zip
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  SYSTEM
)
set(SIMDJSON_ENABLE_THREADS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(simdjson)

# Feed adapters (Coinbase). Header-only handler logic lives in include/feed/;
# this INTERFACE target just wires the simdjson dependency + include path so it
# stays OFF telemetry_core (which must remain dependency-free).
add_library(telemetry_feed INTERFACE)
target_include_directories(telemetry_feed INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(telemetry_feed INTERFACE telemetry_core simdjson::simdjson)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
if(BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

# pybind11 bindings (SPEC-3.10). OFF by default so the core build/test/
# sanitizer trees do not depend on Python or pybind11. Enable with
# -DBUILD_PYTHON_BINDINGS=ON (point Python3_EXECUTABLE at python/.venv).
option(BUILD_PYTHON_BINDINGS "Build the telemetry_engine pybind11 module" OFF)
if(BUILD_PYTHON_BINDINGS)
  add_subdirectory(src/bindings)
endif()

# Live WebSocket tools (NOT ctest tests): connect to the real Coinbase WSS
# endpoint. OFF by default so the core build/test/sanitizer trees do not
# depend on IXWebSocket/TLS.
#   BUILD_LIVE_SMOKE : coinbase_live_smoke, the bounded validation run.
#   BUILD_DAEMON     : coinbase_daemon, the 24/7 deployment daemon
#                      (ingest + detection + stats/alerts files; see
#                      docs/deploy.md).
option(BUILD_LIVE_SMOKE "Build the coinbase_live_smoke WebSocket tool" OFF)
option(BUILD_DAEMON "Build the coinbase_daemon 24/7 deployment daemon" OFF)
if(BUILD_LIVE_SMOKE OR BUILD_DAEMON)
  add_subdirectory(src/feed)
endif()
