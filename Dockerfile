# syntax=docker/dockerfile:1
# Multi-stage deployment image (docs/deploy.md). Target platform is pinned to
# linux/amd64 explicitly: the VPS is x86_64 while the dev machine is ARM —
# build with `docker compose build` (compose pins the platform) or
# `docker buildx build --platform linux/amd64 .`

# ---- build stage: compile the C++ engine daemon (Release, Ninja) -----------
# Same bookworm base as the runtime stage (identical glibc), with a current
# CMake/Ninja from pip: Debian bookworm's cmake 3.25 rejects the SYSTEM
# keyword this project's FetchContent_Declare calls use.
FROM --platform=linux/amd64 python:3.12-slim-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential libssl-dev zlib1g-dev ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir "cmake>=3.28" ninja

WORKDIR /src
COPY CMakeLists.txt ./
COPY include ./include
COPY src ./src

# Tests/benchmarks are OFF here: they run natively and in CI, not per image
# build (and BUILD_TESTS=OFF also skips the googletest fetch entirely).
RUN cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
      -DBUILD_DAEMON=ON -DBUILD_TESTS=OFF -DBUILD_BENCHMARKS=OFF \
    && cmake --build build --target coinbase_daemon

# ---- runtime stage: slim Debian with the daemon + FastAPI status service ---
FROM --platform=linux/amd64 python:3.12-slim-bookworm AS runtime

RUN pip install --no-cache-dir "fastapi>=0.110" "uvicorn>=0.29"

WORKDIR /app
COPY --from=build /src/build/src/feed/coinbase_daemon /app/bin/coinbase_daemon
COPY config/crypto_ticks.yaml config/crypto_eth_usd.yaml /app/config/
COPY python/api/app.py python/api/live.py python/api/serve.py /app/api/
COPY deploy/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

ENV JISA_DATA_DIR=/data
EXPOSE 8000

# /health is 200 only while the daemon's stats are readable (503 before the
# daemon warms up — covered by start-period; urlopen raises on 503 -> exit 1).
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=4)"

ENTRYPOINT ["/app/entrypoint.sh"]
