from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from websocket import WebSocketApp

AISSTREAM_WS_URL = "wss://stream.aisstream.io/v0/stream"
DEFAULT_STATE_FILE = Path("data") / "hormuz_aisstream_state.json"


@dataclass(frozen=True)
class BoundingBox:
    min_lat: float = 25.80
    max_lat: float = 26.70
    min_lon: float = 56.00
    max_lon: float = 56.95


@dataclass(frozen=True)
class MonitorConfig:
    api_key: str
    state_file: Path
    bbox: BoundingBox
    exit_lon: float
    min_speed_knots: float
    min_course_deg: float
    max_course_deg: float
    log_every_messages: int
    reconnect_wait_seconds: int
    filter_message_types: list[str] | None


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def parse_number(raw: Any) -> float | None:
    try:
        val = float(raw)
    except (TypeError, ValueError):
        return None
    if val != val:  # NaN
        return None
    return val


def parse_sog_knots(raw: Any) -> float | None:
    val = parse_number(raw)
    if val is None:
        return None
    # AIS usually emits SOG in 0.1 knots for many payloads.
    if val > 50:
        val = val / 10.0
    if val < 0 or val > 80:
        return None
    return val


def parse_cog_degrees(raw: Any) -> float | None:
    val = parse_number(raw)
    if val is None:
        return None
    # Some feeds may encode COG in 0.1 degrees.
    if val > 360:
        val = val / 10.0
    if val < 0 or val > 360:
        return None
    return val


def parse_mmsi(value: Any) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    return s


def get_ship_type_code(payload: dict[str, Any]) -> int | None:
    for key in ("ShipType", "Type", "TypeAndCargo", "TypeAndCargoType", "CargoType"):
        val = payload.get(key)
        if val is None:
            continue
        try:
            return int(val)
        except (TypeError, ValueError):
            continue
    return None


def get_ship_name(payload: dict[str, Any], metadata: dict[str, Any]) -> str:
    for key in ("Name", "ShipName", "VesselName"):
        val = payload.get(key)
        if val:
            return str(val).strip()
    for key in ("ShipName", "VesselName"):
        val = metadata.get(key)
        if val:
            return str(val).strip()
    return ""


def get_dimensions(payload: dict[str, Any]) -> tuple[float | None, float | None]:
    # AIS Message 5 style
    dim = payload.get("Dimension")
    if isinstance(dim, dict):
        a = parse_number(dim.get("A"))
        b = parse_number(dim.get("B"))
        c = parse_number(dim.get("C"))
        d = parse_number(dim.get("D"))
        if all(x is not None for x in (a, b, c, d)):
            return (a + b, c + d)

    # Flat fields
    a = parse_number(payload.get("DimensionA"))
    b = parse_number(payload.get("DimensionB"))
    c = parse_number(payload.get("DimensionC"))
    d = parse_number(payload.get("DimensionD"))
    if all(x is not None for x in (a, b, c, d)):
        return (a + b, c + d)

    return (None, None)


def is_oil_tanker(ship_type_code: int | None, ship_name: str) -> bool:
    if ship_type_code is not None and 80 <= ship_type_code <= 89:
        # AIS tanker family. With free AIS-only data, this is the best broad proxy.
        return True
    # Extra fallback when type code is missing.
    name = ship_name.lower()
    return (" tanker" in name) or ("oil" in name) or ("crude" in name)


def estimate_capacity_bbl(length_m: float | None) -> float:
    # Heuristic from vessel LOA buckets.
    if length_m is None:
        return 500_000.0
    if length_m >= 320:
        return 2_000_000.0  # VLCC class proxy
    if length_m >= 260:
        return 1_000_000.0  # Suezmax proxy
    if length_m >= 230:
        return 700_000.0  # Aframax proxy
    if length_m >= 180:
        return 500_000.0  # Panamax/LR1 proxy
    if length_m >= 135:
        return 300_000.0  # MR proxy
    return 120_000.0


def build_empty_state() -> dict[str, Any]:
    return {
        "version": 1,
        "created_at_utc": now_utc_iso(),
        "updated_at_utc": now_utc_iso(),
        "last_message_utc": None,
        "daily": {},
        "ships_by_day": {},
        "profiles_by_mmsi": {},
        "stats": {
            "messages_total": 0,
            "position_messages": 0,
            "static_messages": 0,
            "detections_total": 0,
            "reconnections": 0,
            "ws_opens": 0,
            "ws_closes": 0,
            "ws_errors": 0,
        },
        "runtime": {
            "last_open_utc": None,
            "last_close_utc": None,
            "last_close_code": None,
            "last_close_msg": None,
            "last_error_utc": None,
            "last_error": None,
        },
    }


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return build_empty_state()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            return build_empty_state()
        return data
    except Exception:
        return build_empty_state()


def save_state(path: Path, state: dict[str, Any]) -> None:
    state["updated_at_utc"] = now_utc_iso()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def ensure_day(state: dict[str, Any], day: str) -> tuple[dict[str, Any], dict[str, Any]]:
    daily = state.setdefault("daily", {})
    ships_by_day = state.setdefault("ships_by_day", {})
    if day not in daily:
        daily[day] = {
            "ship_count": 0,
            "volume_bbl": 0.0,
            "updated_at_utc": now_utc_iso(),
        }
    if day not in ships_by_day:
        ships_by_day[day] = {}
    return daily[day], ships_by_day[day]


class HormuzAisStreamMonitor:
    def __init__(self, config: MonitorConfig) -> None:
        self.config = config
        self.state = load_state(config.state_file)
        self.last_save_monotonic = time.monotonic()
        self.messages_since_log = 0
        self.running = True

    def run_forever(self) -> None:
        print(f"[{now_utc_iso()}] Monitor AISStream iniciado.")
        print(f"State file: {self.config.state_file}")
        print(
            "BBox: "
            f"{self.config.bbox.min_lat},{self.config.bbox.min_lon} -> "
            f"{self.config.bbox.max_lat},{self.config.bbox.max_lon}"
        )
        while self.running:
            self._run_single_connection()
            self.state["stats"]["reconnections"] = int(
                self.state.get("stats", {}).get("reconnections", 0)
            ) + 1
            save_state(self.config.state_file, self.state)
            print(
                f"[{now_utc_iso()}] Conexao encerrada. Reconectando em "
                f"{self.config.reconnect_wait_seconds}s..."
            )
            time.sleep(self.config.reconnect_wait_seconds)

    def _run_single_connection(self) -> None:
        ws_app = WebSocketApp(
            AISSTREAM_WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        # ping interval helps keep middleboxes alive.
        ws_app.run_forever(ping_interval=20, ping_timeout=10)

    def _on_open(self, ws: WebSocketApp) -> None:
        sub_msg = {
            "APIKey": self.config.api_key,
            "BoundingBoxes": [
                [
                    [self.config.bbox.min_lat, self.config.bbox.min_lon],
                    [self.config.bbox.max_lat, self.config.bbox.max_lon],
                ]
            ],
        }
        if self.config.filter_message_types:
            sub_msg["FilterMessageTypes"] = self.config.filter_message_types
        ws.send(json.dumps(sub_msg))
        stats = self.state.setdefault("stats", {})
        stats["ws_opens"] = int(stats.get("ws_opens", 0)) + 1
        runtime = self.state.setdefault("runtime", {})
        runtime["last_open_utc"] = now_utc_iso()
        print(f"[{now_utc_iso()}] Conectado ao AISStream e inscricao enviada.")

    def _on_message(self, _ws: WebSocketApp, message_raw: str) -> None:
        self.state["last_message_utc"] = now_utc_iso()
        stats = self.state.setdefault("stats", {})
        stats["messages_total"] = int(stats.get("messages_total", 0)) + 1
        self.messages_since_log += 1

        try:
            msg = json.loads(message_raw)
        except json.JSONDecodeError:
            return

        if isinstance(msg, dict) and msg.get("error"):
            print(f"[{now_utc_iso()}] AISStream erro: {msg.get('error')}")
            return

        if not isinstance(msg, dict):
            return

        message_type = str(msg.get("MessageType", "")).strip()
        metadata = msg.get("MetaData") or msg.get("Metadata") or {}
        if not isinstance(metadata, dict):
            metadata = {}
        envelope = msg.get("Message") or {}
        if not isinstance(envelope, dict):
            return
        payload = envelope.get(message_type)
        if not isinstance(payload, dict):
            # Fallback: first dict in envelope.
            for val in envelope.values():
                if isinstance(val, dict):
                    payload = val
                    break
        if not isinstance(payload, dict):
            return

        if message_type in {"ShipStaticData", "StaticDataReport"}:
            stats["static_messages"] = int(stats.get("static_messages", 0)) + 1
            self._update_profile_from_static(payload, metadata)
        elif message_type in {
            "PositionReport",
            "StandardClassBPositionReport",
            "ExtendedClassBPositionReport",
        }:
            stats["position_messages"] = int(stats.get("position_messages", 0)) + 1
            self._process_position(payload, metadata)

        if self.messages_since_log >= self.config.log_every_messages:
            self.messages_since_log = 0
            today = utc_day()
            day_rec = self.state.get("daily", {}).get(today, {})
            print(
                f"[{now_utc_iso()}] msgs={stats.get('messages_total', 0)} "
                f"today_ships={day_rec.get('ship_count', 0)} "
                f"today_vol_bbl={int(day_rec.get('volume_bbl', 0))}"
            )

        if (time.monotonic() - self.last_save_monotonic) >= 15:
            save_state(self.config.state_file, self.state)
            self.last_save_monotonic = time.monotonic()

    def _update_profile_from_static(
        self, payload: dict[str, Any], metadata: dict[str, Any]
    ) -> None:
        mmsi = parse_mmsi(
            payload.get("UserID")
            or payload.get("MMSI")
            or metadata.get("MMSI")
            or metadata.get("UserID")
        )
        if not mmsi:
            return

        ship_type_code = get_ship_type_code(payload)
        ship_name = get_ship_name(payload, metadata)
        length_m, beam_m = get_dimensions(payload)
        profile = {
            "mmsi": mmsi,
            "ship_name": ship_name,
            "ship_type_code": ship_type_code,
            "length_m": length_m,
            "beam_m": beam_m,
            "is_oil_tanker": is_oil_tanker(ship_type_code, ship_name),
            "updated_at_utc": now_utc_iso(),
        }
        self.state.setdefault("profiles_by_mmsi", {})[mmsi] = profile

    def _process_position(self, payload: dict[str, Any], metadata: dict[str, Any]) -> None:
        mmsi = parse_mmsi(
            payload.get("UserID")
            or payload.get("MMSI")
            or metadata.get("MMSI")
            or metadata.get("UserID")
        )
        if not mmsi:
            return

        lat = parse_number(payload.get("Latitude"))
        lon = parse_number(payload.get("Longitude"))
        if lat is None:
            lat = parse_number(metadata.get("Latitude"))
        if lon is None:
            lon = parse_number(metadata.get("Longitude"))
        if lat is None or lon is None:
            return

        sog = parse_sog_knots(payload.get("Sog") or payload.get("SOG"))
        cog = parse_cog_degrees(payload.get("Cog") or payload.get("COG"))
        if sog is None or cog is None:
            return

        if sog < self.config.min_speed_knots:
            return
        if lon < self.config.exit_lon:
            return
        if cog < self.config.min_course_deg or cog > self.config.max_course_deg:
            return

        profile = self.state.get("profiles_by_mmsi", {}).get(mmsi)
        if not profile:
            # If we do not have static details yet, wait for static message.
            return
        if not bool(profile.get("is_oil_tanker")):
            return

        day = utc_day()
        day_rec, day_ships = ensure_day(self.state, day)
        if mmsi in day_ships:
            return

        length_m = parse_number(profile.get("length_m"))
        capacity_bbl = estimate_capacity_bbl(length_m)
        day_ships[mmsi] = {
            "mmsi": mmsi,
            "ship_name": str(profile.get("ship_name", "")).strip(),
            "ship_type_code": profile.get("ship_type_code"),
            "length_m": length_m,
            "capacity_bbl_est": capacity_bbl,
            "detected_at_utc": now_utc_iso(),
            "lat": lat,
            "lon": lon,
            "sog_knots": sog,
            "cog_deg": cog,
        }
        day_rec["ship_count"] = int(day_rec.get("ship_count", 0)) + 1
        day_rec["volume_bbl"] = float(day_rec.get("volume_bbl", 0.0)) + capacity_bbl
        day_rec["updated_at_utc"] = now_utc_iso()

        stats = self.state.setdefault("stats", {})
        stats["detections_total"] = int(stats.get("detections_total", 0)) + 1
        save_state(self.config.state_file, self.state)
        self.last_save_monotonic = time.monotonic()
        print(
            f"[{now_utc_iso()}] Deteccao: MMSI={mmsi} "
            f"ship='{profile.get('ship_name', '')}' cap~{int(capacity_bbl)} bbl "
            f"(day_count={day_rec['ship_count']})"
        )

    def _on_error(self, _ws: WebSocketApp, error: Any) -> None:
        stats = self.state.setdefault("stats", {})
        stats["ws_errors"] = int(stats.get("ws_errors", 0)) + 1
        runtime = self.state.setdefault("runtime", {})
        runtime["last_error_utc"] = now_utc_iso()
        runtime["last_error"] = str(error)
        save_state(self.config.state_file, self.state)
        print(f"[{now_utc_iso()}] WebSocket erro: {error}")

    def _on_close(
        self, _ws: WebSocketApp, status_code: int | None, message: str | None
    ) -> None:
        stats = self.state.setdefault("stats", {})
        stats["ws_closes"] = int(stats.get("ws_closes", 0)) + 1
        runtime = self.state.setdefault("runtime", {})
        runtime["last_close_utc"] = now_utc_iso()
        runtime["last_close_code"] = status_code
        runtime["last_close_msg"] = message
        save_state(self.config.state_file, self.state)
        print(f"[{now_utc_iso()}] WebSocket fechado: code={status_code} msg={message}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Monitor continuo AISStream para contar petroleiros saindo de Hormuz "
            "e acumular historico diario local."
        )
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("AISSTREAM_API_KEY", "").strip(),
        help="AISStream API key (ou env AISSTREAM_API_KEY).",
    )
    parser.add_argument(
        "--state-file",
        type=Path,
        default=DEFAULT_STATE_FILE,
        help="Arquivo JSON local de estado/historico.",
    )
    parser.add_argument("--min-lat", type=float, default=25.80)
    parser.add_argument("--max-lat", type=float, default=26.70)
    parser.add_argument("--min-lon", type=float, default=56.00)
    parser.add_argument("--max-lon", type=float, default=56.95)
    parser.add_argument(
        "--exit-lon",
        type=float,
        default=56.85,
        help="Longitude a leste para considerar navio em saida.",
    )
    parser.add_argument("--min-speed-knots", type=float, default=2.0)
    parser.add_argument("--min-course-deg", type=float, default=45.0)
    parser.add_argument("--max-course-deg", type=float, default=135.0)
    parser.add_argument("--log-every-messages", type=int, default=500)
    parser.add_argument("--reconnect-wait-seconds", type=int, default=5)
    parser.add_argument(
        "--filter-message-type",
        action="append",
        default=[],
        help=(
            "Filtro opcional de tipo AIS (pode repetir varias vezes). "
            "Ex.: --filter-message-type PositionReport"
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.api_key:
        print("API key ausente. Defina --api-key ou AISSTREAM_API_KEY.")
        return 2

    bbox = BoundingBox(
        min_lat=args.min_lat,
        max_lat=args.max_lat,
        min_lon=args.min_lon,
        max_lon=args.max_lon,
    )
    cfg = MonitorConfig(
        api_key=args.api_key,
        state_file=args.state_file,
        bbox=bbox,
        exit_lon=args.exit_lon,
        min_speed_knots=args.min_speed_knots,
        min_course_deg=args.min_course_deg,
        max_course_deg=args.max_course_deg,
        log_every_messages=max(1, args.log_every_messages),
        reconnect_wait_seconds=max(1, args.reconnect_wait_seconds),
        filter_message_types=(args.filter_message_type or None),
    )
    monitor = HormuzAisStreamMonitor(cfg)
    monitor.run_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
