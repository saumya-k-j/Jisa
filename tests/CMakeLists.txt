# Tests mirror src/ layout: tests/core/... for include/core/...
include(GoogleTest)

function(telemetry_add_test name src)
  add_executable(${name} ${src})
  target_link_libraries(${name} PRIVATE telemetry_core telemetry_warnings GTest::gtest_main)
  gtest_discover_tests(${name})
endfunction()

telemetry_add_test(test_message core/test_message.cpp)
telemetry_add_test(test_ring_buffer core/test_ring_buffer.cpp)
telemetry_add_test(test_pool_allocator core/test_pool_allocator.cpp)

# Phase 2: feed/handler (crypto/Coinbase adapter). Mirrors include/feed/.
# FIXTURE_DIR points at the recorded real-message fixture used by all feed
# tests (see tests/feed/fixture_utils.hpp).
function(telemetry_add_feed_test name src)
  telemetry_add_test(${name} ${src})
  # Feed tests include feed/coinbase.hpp (simdjson) -> link the feed library.
  # telemetry_feed is defined in the root CMakeLists and pulls in simdjson;
  # telemetry_core itself stays dependency-free.
  target_link_libraries(${name} PRIVATE telemetry_feed)
  target_include_directories(${name} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/feed)
  target_compile_definitions(${name} PRIVATE FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/feed/fixtures")
endfunction()

telemetry_add_feed_test(test_coinbase_parse feed/test_coinbase_parse.cpp)
telemetry_add_feed_test(test_gap_detection feed/test_gap_detection.cpp)
telemetry_add_feed_test(test_reconnect feed/test_reconnect.cpp)
telemetry_add_feed_test(test_end_to_end feed/test_end_to_end.cpp)

# Phase 6: remaining two adapters (grid frequency + ADS-B), parallel
# implementations. Mirrors include/feed/{gridradar,adsb}.hpp, which do NOT
# exist yet -- these targets are expected to FAIL TO BUILD until the
# implementer agent adds them. Same feed-test linking pattern as above
# (telemetry_feed for simdjson, FIXTURE_DIR for the recorded/synthetic
# fixtures).
telemetry_add_feed_test(test_gridradar feed/test_gridradar.cpp)
telemetry_add_feed_test(test_adsb feed/test_adsb.cpp)

# Phase 3: replay + fault injection. Mirrors include/replay/. Each new test
# links telemetry_core only (see include/replay/*.hpp: header-only, no
# simdjson dependency), matching the phase-1 core tests above.
telemetry_add_test(test_checksum replay/test_checksum.cpp)
telemetry_add_test(test_recorder_replayer replay/test_recorder_replayer.cpp)
telemetry_add_test(test_replay_determinism replay/test_replay_determinism.cpp)
# The determinism test deliberately uses a `volatile int` accumulator to
# simulate uneven caller-side pacing between pulls; `busy += k` is a compound
# assignment to a volatile, which C++20 deprecates (-Wdeprecated-volatile),
# fatal under the project's -Werror. Scope the suppression to this one test
# target only -- project/hot-path code keeps the full strict warning set.
target_compile_options(test_replay_determinism PRIVATE -Wno-deprecated-volatile)
telemetry_add_test(test_replay_errors replay/test_replay_errors.cpp)
telemetry_add_test(test_fault_injector replay/test_fault_injector.cpp)

# test_recorder_replayer additionally parses the real Coinbase fixture via
# CoinbaseTickerHandler -> needs telemetry_feed (simdjson) plus the shared
# feed fixture reader / fixture dir, following the same linking pattern as
# telemetry_add_feed_test() above.
target_link_libraries(test_recorder_replayer PRIVATE telemetry_feed)
target_include_directories(test_recorder_replayer PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/feed)
target_compile_definitions(test_recorder_replayer PRIVATE FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/feed/fixtures")

# Phase 4: detect layers 1-3 (rules, baseline EWMA, CUSUM changepoint).
# Mirrors include/detect/, which does NOT exist yet -- these targets are
# expected to FAIL TO BUILD until the implementer agent adds
# include/detect/{rules,baseline,cusum}.hpp. Link telemetry_core only (no
# simdjson dependency), matching the phase-1/3 pattern above.
telemetry_add_test(test_rules detect/test_rules.cpp)
target_compile_definitions(test_rules PRIVATE
  DETECT_FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/detect/fixtures"
  CONFIG_DIR="${CMAKE_CURRENT_SOURCE_DIR}/../config")

telemetry_add_test(test_baseline detect/test_baseline.cpp)
telemetry_add_test(test_cusum detect/test_cusum.cpp)

# End-to-end detection-delay measurement (objective #3): ties CUSUM to the
# already-DONE replay + fault injection module. Needs replay/*.hpp (header
# only, no simdjson), same as test_fault_injector above.
telemetry_add_test(test_detection_delay detect/test_detection_delay.cpp)

# Phase 5: detect layer 4 (conformal thresholds) + empirical false-alarm-rate
# verification (measurable objective #4). Mirrors include/detect/, which
# does NOT yet have conformal.hpp -- these targets are expected to FAIL TO
# BUILD until the implementer agent adds include/detect/conformal.hpp. Link
# telemetry_core only (no simdjson dependency), matching the phase-4 pattern
# above. test_false_alarm_rate.cpp additionally exercises the real,
# already-implemented EwmaBaseline (detect/baseline.hpp) as its
# nonconformity-score source.
telemetry_add_test(test_conformal detect/test_conformal.cpp)
telemetry_add_test(test_false_alarm_rate detect/test_false_alarm_rate.cpp)

# ALLOCATION-AUDIT: upgrades the CLAUDE.md hot-path "zero heap allocation
# after warmup" claim from design-verified to test-verified by counting real
# allocations (global operator new/delete overrides local to this one test
# binary). Lives in tests/core/ (mirrors include/core/, its primary subject:
# SpscRingBuffer + PoolAllocator) but also exercises feed/coinbase.hpp and
# detect/*.hpp, so it needs the same telemetry_feed / FIXTURE_DIR wiring as
# test_recorder_replayer above rather than the plain telemetry_add_test path.
#
# Excluded from sanitizer builds (SANITIZE != off): the audit's global
# operator new/delete overrides collide at link time with the ASan/TSan
# runtimes (libclang_rt.*_cxx also defines these symbols -> "multiple
# definition of operator new" on Linux; macOS only links by accident of a
# different runtime layout, see DECISIONS.md). The audit itself is fully
# built and verified in the Release tree -- this is exclusion, not weakening.
if(SANITIZE STREQUAL "off")
  telemetry_add_test(test_hot_path_allocation core/test_hot_path_allocation.cpp)
  target_link_libraries(test_hot_path_allocation PRIVATE telemetry_feed)
  target_include_directories(test_hot_path_allocation PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/feed)
  target_compile_definitions(test_hot_path_allocation PRIVATE FIXTURE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/feed/fixtures")
endif()
