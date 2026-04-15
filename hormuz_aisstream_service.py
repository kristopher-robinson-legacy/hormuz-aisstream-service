from __future__ import annotations

import json
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify

from hormuz_aisstream_monitor import (
    BoundingBox,
    HormuzAisStreamMonitor,
    MonitorConfig,
    load_state,
)

DEFAULT_STATE_FILE = Path(os.getenv("HORMUZ_STATE_FILE", "data/hormuz_aisstream_state.json"))

app = Flask(__name__)
_monitor_thread: threading.Thread | None = None
_monitor_started = False
_monitor_error: str | None = None


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _build_config() -> MonitorConfig:
    api_key = os.getenv("AISSTREAM_API_KEY", "").strip()
    bbox = BoundingBox(
        min_lat=float(os.getenv("HORMUZ_MIN_LAT", "25.80")),
        max_lat=float(os.getenv("HORMUZ_MAX_LAT", "26.70")),
        min_lon=float(os.getenv("HORMUZ_MIN_LON", "56.00")),
        max_lon=float(os.getenv("HORMUZ_MAX_LON", "56.95")),
    )
    return MonitorConfig(
        api_key=api_key,
        state_file=DEFAULT_STATE_FILE,
        bbox=bbox,
        exit_lon=float(os.getenv("HORMUZ_EXIT_LON", "56.85")),
        min_speed_knots=float(os.getenv("HORMUZ_MIN_SPEED_KNOTS", "2.0")),
        min_course_deg=float(os.getenv("HORMUZ_MIN_COURSE_DEG", "45.0")),
        max_course_deg=float(os.getenv("HORMUZ_MAX_COURSE_DEG", "135.0")),
        log_every_messages=int(os.getenv("HORMUZ_LOG_EVERY_MESSAGES", "500")),
        reconnect_wait_seconds=int(os.getenv("HORMUZ_RECONNECT_WAIT_SECONDS", "5")),
    )


def _start_monitor_once() -> None:
    global _monitor_thread, _monitor_started, _monitor_error
    if _monitor_started:
        return
    cfg = _build_config()
    if not cfg.api_key:
        _monitor_error = "AISSTREAM_API_KEY ausente."
        return

    monitor = HormuzAisStreamMonitor(cfg)

    def _runner() -> None:
        nonlocal monitor
        monitor.run_forever()

    _monitor_thread = threading.Thread(
        target=_runner,
        name="hormuz-aisstream-monitor",
        daemon=True,
    )
    _monitor_thread.start()
    _monitor_started = True


@app.after_request
def _set_cors_headers(resp: Response) -> Response:
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
    resp.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return resp


@app.get("/")
def root() -> Any:
    _start_monitor_once()
    return jsonify(
        {
            "service": "hormuz-aisstream-service",
            "now_utc": now_utc_iso(),
            "monitor_started": _monitor_started,
            "monitor_error": _monitor_error,
            "state_file": str(DEFAULT_STATE_FILE),
            "endpoints": ["/health", "/state", "/daily", "/today"],
        }
    )


@app.get("/health")
def health() -> Any:
    _start_monitor_once()
    state = load_state(DEFAULT_STATE_FILE)
    stats = state.get("stats", {}) if isinstance(state, dict) else {}
    return jsonify(
        {
            "ok": True,
            "now_utc": now_utc_iso(),
            "monitor_started": _monitor_started,
            "monitor_error": _monitor_error,
            "last_message_utc": state.get("last_message_utc") if isinstance(state, dict) else None,
            "messages_total": int(stats.get("messages_total", 0)),
            "detections_total": int(stats.get("detections_total", 0)),
        }
    )


@app.get("/state")
def state() -> Any:
    _start_monitor_once()
    payload = load_state(DEFAULT_STATE_FILE)
    if not isinstance(payload, dict):
        payload = {}
    payload["_service"] = {
        "now_utc": now_utc_iso(),
        "monitor_started": _monitor_started,
        "monitor_error": _monitor_error,
    }
    return Response(
        json.dumps(payload, ensure_ascii=False),
        status=200,
        mimetype="application/json",
    )


@app.get("/daily")
def daily() -> Any:
    _start_monitor_once()
    payload = load_state(DEFAULT_STATE_FILE)
    if not isinstance(payload, dict):
        payload = {}
    daily_map = payload.get("daily", {})
    if not isinstance(daily_map, dict):
        daily_map = {}
    rows: list[dict[str, Any]] = []
    for day, rec in daily_map.items():
        if not isinstance(rec, dict):
            continue
        rows.append(
            {
                "day_utc": day,
                "ship_count": int(rec.get("ship_count", 0)),
                "volume_bbl": float(rec.get("volume_bbl", 0.0)),
                "updated_at_utc": rec.get("updated_at_utc"),
            }
        )
    rows.sort(key=lambda r: r["day_utc"], reverse=True)
    return jsonify({"rows": rows, "now_utc": now_utc_iso()})


@app.get("/today")
def today() -> Any:
    _start_monitor_once()
    payload = load_state(DEFAULT_STATE_FILE)
    if not isinstance(payload, dict):
        payload = {}
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    daily_map = payload.get("daily", {})
    ships_by_day = payload.get("ships_by_day", {})
    rec = daily_map.get(day, {}) if isinstance(daily_map, dict) else {}
    ships = ships_by_day.get(day, {}) if isinstance(ships_by_day, dict) else {}
    if not isinstance(rec, dict):
        rec = {}
    if not isinstance(ships, dict):
        ships = {}
    return jsonify(
        {
            "day_utc": day,
            "ship_count": int(rec.get("ship_count", 0)),
            "volume_bbl": float(rec.get("volume_bbl", 0.0)),
            "updated_at_utc": rec.get("updated_at_utc"),
            "ships": list(ships.values()),
            "now_utc": now_utc_iso(),
        }
    )


if __name__ == "__main__":
    _start_monitor_once()
    port = int(os.getenv("PORT", "8000"))
    app.run(host="0.0.0.0", port=port)
