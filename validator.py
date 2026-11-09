"""MQTT 페이로드 검증. DB/네트워크와 분리된 순수 함수라 단위 테스트가 쉽다."""
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone

# sensor_id 접두어 → (센서 종류, 최소, 최대)
SENSOR_SPECS = {
    "temp": ("temperature", -40.0, 125.0),  # ℃
    "hum": ("humidity", 0.0, 100.0),        # %RH
    "cur": ("current", 0.0, 50.0),          # A
}
REQUIRED_FIELDS = ("message_id", "sensor_id", "value", "timestamp")


@dataclass
class Reading:
    message_id: str
    sensor_id: str
    sensor_type: str
    value: float
    measured_at: datetime  # UTC, tz 정보 제거(DATETIME2 저장용)


@dataclass
class Rejection:
    reason: str
    detail: str
    message_id: str | None = None


def validate(payload: bytes | str) -> Reading | Rejection:
    try:
        msg = json.loads(payload)
    except (ValueError, UnicodeDecodeError) as e:
        return Rejection("invalid_json", str(e)[:200])
    if not isinstance(msg, dict):
        return Rejection("invalid_json", "payload is not a JSON object")

    mid = msg.get("message_id") if isinstance(msg.get("message_id"), str) else None

    missing = [f for f in REQUIRED_FIELDS if msg.get(f) is None]
    if missing:
        return Rejection("missing_field", f"missing: {', '.join(missing)}", mid)

    if not isinstance(msg["message_id"], str) or not 0 < len(msg["message_id"]) <= 64:
        return Rejection("invalid_type", "message_id must be a 1~64 char string", mid)
    if not isinstance(msg["sensor_id"], str):
        return Rejection("invalid_type", "sensor_id must be a string", mid)

    prefix = msg["sensor_id"].split("-")[0]
    if prefix not in SENSOR_SPECS:
        return Rejection("unknown_sensor", f"unknown sensor prefix: {prefix!r}", mid)
    sensor_type, lo, hi = SENSOR_SPECS[prefix]

    value = msg["value"]
    # bool은 int의 하위 타입이라 명시적으로 제외, NaN/Infinity도 제외
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        return Rejection("invalid_type", f"value must be a finite number, got {value!r}"[:200], mid)

    try:
        ts = datetime.fromisoformat(msg["timestamp"])
    except (TypeError, ValueError):
        return Rejection("invalid_type", f"timestamp is not ISO 8601: {msg['timestamp']!r}"[:200], mid)
    if ts.tzinfo is None:
        return Rejection("invalid_type", "timestamp must include timezone", mid)

    if not lo <= value <= hi:
        return Rejection("out_of_range", f"{sensor_type} {value} not in [{lo}, {hi}]", mid)

    return Reading(
        message_id=msg["message_id"],
        sensor_id=msg["sensor_id"],
        sensor_type=sensor_type,
        value=float(value),
        measured_at=ts.astimezone(timezone.utc).replace(tzinfo=None),
    )
