# Live smoke tool only. Pulls in IXWebSocket (WSS/TLS) via FetchContent.
# SYSTEM so IXWebSocket headers are not subject to our -Werror. TLS enabled;
# on macOS IXWebSocket uses Secure Transport by default (no OpenSSL needed).
include(FetchContent)

set(USE_TLS ON CACHE BOOL "" FORCE)
set(IXWEBSOCKET_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
  ixwebsocket
  URL https://github.com/machinezone/IXWebSocket/archive/refs/tags/v11.4.5.zip
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  SYSTEM
)
FetchContent_MakeAvailable(ixwebsocket)

add_executable(coinbase_live_smoke coinbase_live_main.cpp)
# NOTE: no telemetry_warnings here — the smoke tool is validation scaffolding,
# not hot-path code, and IXWebSocket headers are not -Werror clean.
target_link_libraries(coinbase_live_smoke PRIVATE telemetry_feed ixwebsocket)
