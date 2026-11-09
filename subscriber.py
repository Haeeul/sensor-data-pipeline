"""MQTT 구독 → 검증 → MSSQL 배치 저장.

구조: [MQTT 네트워크 스레드] --(queue)--> [writer 스레드: 검증 + 배치 INSERT + ACK]
"""
import logging
import queue
import threading
import time

import paho.mqtt.client as mqtt
import pymssql

import config
import db
from validator import Reading, Rejection, validate

log = logging.getLogger("subscriber")

# SQL Server는 쿼리당 파라미터 2100개 제한 → 행당 파라미터 수로 나눠 청크 크기 결정
READING_CHUNK = 2000 // 5
REJECT_CHUNK = 2000 // 4

inbox: "queue.Queue[tuple[int, bytes]]" = queue.Queue()
stats = {"received": 0, "stored": 0, "rejected": 0}
stop = threading.Event()


def insert_readings(cur, readings: list[Reading]):
    for i in range(0, len(readings), READING_CHUNK):
        chunk = readings[i:i + READING_CHUNK]
        values = ",".join(["(%s,%s,%s,%s,%s)"] * len(chunk))
        params = [p for r in chunk for p in (r.message_id, r.sensor_id, r.sensor_type, r.value, r.measured_at)]
        cur.execute(
            "INSERT INTO sensor_readings (message_id, sensor_id, sensor_type, value, measured_at) "
            f"VALUES {values}", tuple(params))


def insert_rejects(cur, rejects: list[tuple[Rejection, str]]):
    for i in range(0, len(rejects), REJECT_CHUNK):
        chunk = rejects[i:i + REJECT_CHUNK]
        values = ",".join(["(%s,%s,%s,%s)"] * len(chunk))
        params = [p for rej, raw in chunk for p in (rej.message_id, rej.reason, rej.detail, raw)]
        cur.execute(
            f"INSERT INTO rejected_messages (message_id, reason, detail, raw_payload) VALUES {values}",
            tuple(params))


def save_batch(conn, batch: list[tuple[int, bytes]]):
    readings: list[Reading] = []
    rejects: list[tuple[Rejection, str]] = []
    seen: set[str] = set()
    for _, payload in batch:
        result = validate(payload)
        raw = payload.decode("utf-8", errors="replace")
        if isinstance(result, Rejection):
            rejects.append((result, raw))
        elif result.message_id in seen:
            # 배치 내 중복은 메모리에서 먼저 제거 → 느린 1건씩 경로를 타지 않게 함
            rejects.append((Rejection("duplicate", "duplicate within batch", result.message_id), raw))
        else:
            seen.add(result.message_id)
            readings.append(result)

    cur = conn.cursor()
    try:
        insert_readings(cur, readings)  # 빠른 경로: 다건 INSERT 한 번
    except pymssql.IntegrityError:
        # 이전 배치에 이미 저장된 message_id가 섞임 → 이 배치만 한 건씩 넣어 중복 건을 정확히 골라냄
        conn.rollback()
        stored = []
        for r in readings:
            try:
                insert_readings(cur, [r])
                stored.append(r)
            except pymssql.IntegrityError:
                rejects.append((Rejection("duplicate", "message_id already stored", r.message_id),
                                f'{{"message_id": "{r.message_id}"}}'))
        readings = stored

    insert_rejects(cur, rejects)
    conn.commit()
    return len(readings), len(rejects)


def writer_loop(client: mqtt.Client):
    conn = db.connect()
    while not (stop.is_set() and inbox.empty()):
        # BATCH_SIZE가 차거나 FLUSH_INTERVAL이 지나면 저장
        batch, deadline = [], time.monotonic() + config.FLUSH_INTERVAL_SEC
        while len(batch) < config.BATCH_SIZE and (left := deadline - time.monotonic()) > 0:
            try:
                batch.append(inbox.get(timeout=left))
            except queue.Empty:
                break
        if not batch:
            continue

        while True:  # DB 장애 시 ACK하지 않고 재시도 → 메시지가 유실되지 않음
            try:
                n_ok, n_rej = save_batch(conn, batch)
                break
            except pymssql.Error as e:
                log.error("DB error, retry in 2s: %s", e)
                time.sleep(2)
                try:
                    conn.close()
                except Exception:
                    pass
                try:
                    conn = db.connect()
                except pymssql.Error:
                    pass

        # 커밋이 끝난 뒤에야 브로커에 ACK → "DB 저장까지" at-least-once
        for mid, _ in batch:
            client.ack(mid, config.MQTT_QOS)
        stats["stored"] += n_ok
        stats["rejected"] += n_rej
    conn.close()


def on_connect(client, userdata, flags, reason_code, properties):
    log.info("connected (%s), subscribing %s/#", reason_code, config.MQTT_TOPIC)
    client.subscribe(f"{config.MQTT_TOPIC}/#", qos=config.MQTT_QOS)


def on_message(client, userdata, msg):
    # 네트워크 스레드는 큐에 넣기만 함 (DB 지연이 수신을 막지 않도록)
    inbox.put((msg.mid, msg.payload))
    stats["received"] += 1


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    # clean_session=False: 구독자가 잠깐 끊겨도 브로커가 메시지를 보관했다가 재접속 시 전달
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="sdp-subscriber",
                         clean_session=False, manual_ack=True)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(config.MQTT_HOST, config.MQTT_PORT, keepalive=30)

    writer = threading.Thread(target=writer_loop, args=(client,), daemon=True)
    writer.start()
    client.loop_start()
    try:
        while True:
            time.sleep(5)
            log.info("received=%(received)d stored=%(stored)d rejected=%(rejected)d queue=%(q)d",
                     {**stats, "q": inbox.qsize()})
    except KeyboardInterrupt:
        log.info("shutting down, flushing remaining messages...")
        stop.set()
        writer.join(timeout=30)
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
