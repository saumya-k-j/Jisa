"""Deployment entrypoint: serve the FastAPI status service backed by the
coinbase_daemon's data directory.

  JISA_DATA_DIR  daemon --data-dir (default /data)
  JISA_DB_PATH   SQLite alert history (default <JISA_DATA_DIR>/alerts.db)
  JISA_PORT      listen port (default 8000)

Run: python -m uvicorn is not needed; this module calls uvicorn directly:
  python python/api/serve.py
"""
from __future__ import annotations

import os
import sys

import uvicorn

sys.path.insert(0, os.path.dirname(__file__))

from app import create_app  # noqa: E402
from live import LiveEngineSource  # noqa: E402


def main() -> None:
    data_dir = os.environ.get("JISA_DATA_DIR", "/data")
    db_path = os.environ.get("JISA_DB_PATH",
                             os.path.join(data_dir, "alerts.db"))
    port = int(os.environ.get("JISA_PORT", "8000"))

    source = LiveEngineSource(data_dir=data_dir, db_path=db_path)
    app = create_app(source)
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")


if __name__ == "__main__":
    main()
