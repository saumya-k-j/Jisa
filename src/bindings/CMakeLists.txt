# SPEC-3.10: pybind11 extension module `telemetry_engine`. Built ONLY when
# -DBUILD_PYTHON_BINDINGS=ON (OFF by default, so the core build/test/sanitizer
# trees never need a Python interpreter or pybind11).
#
# pybind11 is pulled via FetchContent with a pinned tag and marked SYSTEM so its
# headers are included with -isystem and our -Wall -Wextra -Werror does not
# apply to them (mirrors the googletest / simdjson entries in the root
# CMakeLists). telemetry_warnings IS applied to bindings.cpp itself.

include(FetchContent)

FetchContent_Declare(
  pybind11
  URL https://github.com/pybind/pybind11/archive/refs/tags/v3.0.1.zip
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  SYSTEM
)
FetchContent_MakeAvailable(pybind11)

pybind11_add_module(telemetry_engine bindings.cpp)
target_link_libraries(telemetry_engine PRIVATE telemetry_core telemetry_warnings)
