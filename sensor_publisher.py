"""가상 센서 발행기. 정상 데이터 + 의도적 이상 데이터를 MQTT로 전송한다.

예) python sensor_publisher.py --sensors 6 --interval 1 --count 100 --anomaly-rate 0.1
"""
import argparse
import json
import random
import time
import uuid
from collections import Counter
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

import config
from validator import SENSOR_SPECS

TYPES = list(SENSOR_SPECS)  # temp, hum, cur
NORMAL = {"temp": (25, 3), "hum": (50, 8), "cur": (5, 1.5)}  # 정상값 분포 (평균, 표준편차)
ANOMALIES = ["missing_field", "duplicate", "invalid_type", "out_of_range"]


def normal_message(sensor_id: str, message_id: str) -> dict:
    mean, sd = NORMAL[sensor_id.split("-")[0]]
    _, lo, hi = SENSOR_SPECS[sensor_id.split("-")[0]]
    return {
        "message_id": message_id,
        "sensor_id": sensor_id,
        "value": round(min(max(random.gauss(mean, sd), lo), hi), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def corrupt(msg: dict, kind: str) -> dict:
    msg = dict(msg)
    if kind == "missing_field":
        del msg[random.choice(["message_id", "sensor_id", "value", "timestamp"])]
    elif kind == "invalid_type":
        if random.random() < 0.5:
            msg["value"] = str(msg["value"])
        else:
            msg["timestamp"] = "not-a-time"
    elif kind == "out_of_range":
        _, lo, hi = SENSOR_SPECS[msg["sensor_id"].split("-")[0]]
        msg["value"] = hi + random.uniform(1, 100) if random.random() < 0.5 else lo - random.uniform(1, 100)
    return msg


def run(sensors=6, interval=1.0, count=100, anomaly_rate=0.05, seed=None, run_id=None, quiet=False) -> dict:
    """count건을 발행하고 종류별 건수를 반환 (experiment.py가 기대값 계산에 사용)."""
    rng_seed = seed if seed is not None else time.time_ns()
    random.seed(rng_seed)
    run_id = run_id or uuid.uuid4().hex[:8]
    sensor_ids = [f"{TYPES[i % len(TYPES)]}-{i // len(TYPES) + 1:02d}" for i in range(sensors)]

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"sdp-pub-{run_id}")
    client.max_inflight_messages_set(1000)
    client.connect(config.MQTT_HOST, config.MQTT_PORT)
    client.loop_start()

    kinds = Counter()
    infos = []
    last_valid = None
    seq = 0
    while seq < count:
        for sensor_id in sensor_ids:
            if seq >= count:
                break
            seq += 1
            msg = normal_message(sensor_id, f"{run_id}-{seq:07d}")
            kind = random.choice(ANOMALIES) if random.random() < anomaly_rate else "normal"
            if kind == "duplicate":
                if last_valid is None:
                    kind = "normal"
                else:
                    msg = last_valid  # 직전 정상 메시지를 message_id 그대로 재전송
            elif kind != "normal":
                msg = corrupt(msg, kind)
            if kind == "normal":
                last_valid = msg
            kinds[kind] += 1
            topic = f"{config.MQTT_TOPIC}/{sensor_id.split('-')[0]}/{sensor_id}"
            infos.append(client.publish(topic, json.dumps(msg), qos=config.MQTT_QOS))
        if interval > 0 and seq < count:
            time.sleep(interval)

    for info in infos:  # 브로커 PUBACK까지 대기 → "전송 완료"의 기준
        info.wait_for_publish()
    client.loop_stop()
    client.disconnect()

    summary = {"run_id": run_id, "seed": rng_seed, "sent": seq, **{k: kinds[k] for k in ["normal", *ANOMALIES]}}
    if not quiet:
        print(json.dumps(summary))
    return summary


def main():
    p = argparse.ArgumentParser(description="Virtual sensor MQTT publisher")
    p.add_argument("--sensors", type=int, default=6, help="센서 수 (temp/hum/cur 순환 배정)")
    p.add_argument("--interval", type=float, default=1.0, help="라운드 간 전송 주기(초), 0이면 최대 속도")
    p.add_argument("--count", type=int, default=100, help="총 전송 건수")
    p.add_argument("--anomaly-rate", type=float, default=0.05, help="이상 데이터 비율 (0~1)")
    p.add_argument("--seed", type=int, default=None, help="재현용 난수 시드")
    a = p.parse_args()
    run(a.sensors, a.interval, a.count, a.anomaly_rate, a.seed)


if __name__ == "__main__":
    main()
