import json
import sys
from datetime import datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from validator import Reading, Rejection, validate  # noqa: E402

BASE = {
    "message_id": "m-1",
    "sensor_id": "temp-01",
    "value": 23.5,
    "timestamp": "2026-11-08T10:00:00+09:00",
}


def make(**changes):
    msg = {**BASE, **changes}
    return json.dumps({k: v for k, v in msg.items() if v is not ...})


def test_valid_reading_is_converted_to_utc():
    r = validate(make())
    assert isinstance(r, Reading)
    assert r.sensor_type == "temperature"
    assert r.measured_at == datetime(2026, 11, 8, 1, 0, 0)


@pytest.mark.parametrize("payload, reason", [
    (b"{not json", "invalid_json"),
    (b"[1, 2]", "invalid_json"),
    (make(value=...), "missing_field"),
    (make(timestamp=None), "missing_field"),
    (make(value="23.5"), "invalid_type"),
    (make(value=True), "invalid_type"),
    ('{"message_id":"m","sensor_id":"temp-1","value":NaN,"timestamp":"2026-11-08T00:00:00Z"}', "invalid_type"),
    (make(timestamp="yesterday"), "invalid_type"),
    (make(timestamp="2026-11-08T10:00:00"), "invalid_type"),
    (make(sensor_id="pres-01"), "unknown_sensor"),
    (make(value=200), "out_of_range"),
    (make(sensor_id="hum-01", value=-1), "out_of_range"),
    (make(sensor_id="cur-01", value=50.1), "out_of_range"),
])
def test_rejections(payload, reason):
    r = validate(payload)
    assert isinstance(r, Rejection)
    assert r.reason == reason


def test_rejection_keeps_message_id_when_available():
    assert validate(make(value=999)).message_id == "m-1"
    assert validate(b"{bad").message_id is None


def test_boundary_values_are_valid():
    assert isinstance(validate(make(value=-40)), Reading)
    assert isinstance(validate(make(sensor_id="hum-01", value=100)), Reading)
