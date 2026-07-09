# Live WebSocket tools. Pull in IXWebSocket (WSS/TLS) via FetchContent.
# SYSTEM so IXWebSocket headers are not subject to our -Werror. TLS enabled;
# on macOS IXWebSocket uses Secure Transport by default (no OpenSSL needed);
# on Linux it needs OpenSSL (libssl-dev — see Dockerfile build stage).
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

# NOTE: no telemetry_warnings on these targets — they are validation/
# deployment scaffolding, not hot-path code, and IXWebSocket headers are not
# -Werror clean.
if(BUILD_LIVE_SMOKE)
  add_executable(coinbase_live_smoke coinbase_live_main.cpp)
  target_link_libraries(coinbase_live_smoke PRIVATE telemetry_feed ixwebsocket)
endif()

if(BUILD_DAEMON)
  add_executable(coinbase_daemon coinbase_daemon_main.cpp)
  target_link_libraries(coinbase_daemon PRIVATE telemetry_feed ixwebsocket)
endif()
