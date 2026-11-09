"""전송량을 단계별로 늘려가며 파이프라인 처리량·유실을 측정한다.

예) python experiment.py --stages 1000 10000 50000
"""
import argparse
import csv
import subprocess
import sys
import time
from pathlib import Path

import db
import sensor_publisher

ROOT = Path(__file__).parent
COLUMNS = ["sent", "stored", "rejected", "lost", "expected_stored", "accurate",
           "publish_sec", "elapsed_sec", "msgs_per_sec"]


def wait_until_drained(sent: int, timeout: float, stall_sec: float = 10.0) -> dict:
    """저장+거부 건수가 전송 건수에 도달하거나, stall_sec 동안 변화가 없으면 종료."""
    deadline = time.monotonic() + timeout
    last_total, last_change = -1, time.monotonic()
    while True:
        c = db.counts()
        total = c["stored"] + c["rejected"]
        now = time.monotonic()
        if total >= sent or now > deadline or now - last_change > stall_sec:
            return c
        if total != last_total:
            last_total, last_change = total, now
        time.sleep(0.2)


def run_stage(n: int, args) -> dict:
    db.reset_tables()
    t0 = time.perf_counter()
    pub = sensor_publisher.run(sensors=args.sensors, interval=0, count=n,
                               anomaly_rate=args.anomaly_rate, seed=args.seed, quiet=True)
    t_pub = time.perf_counter() - t0
    c = wait_until_drained(pub["sent"], args.timeout)
    elapsed = time.perf_counter() - t0  # 첫 발행 ~ 마지막 건 DB 반영까지
    return {
        "sent": pub["sent"],
        "stored": c["stored"],
        "rejected": c["rejected"],
        "lost": pub["sent"] - c["stored"] - c["rejected"],
        "expected_stored": pub["normal"],
        # 정상 건수와 사유별 거부 건수가 발행기 집계와 정확히 일치하는지
        "accurate": c["stored"] == pub["normal"] and all(
            c["by_reason"].get(k, 0) == pub[k] for k in sensor_publisher.ANOMALIES),
        "publish_sec": round(t_pub, 2),
        "elapsed_sec": round(elapsed, 2),
        "msgs_per_sec": round(pub["sent"] / elapsed),
    }


def print_table(rows: list[dict]):
    widths = {c: max(len(c), *(len(str(r[c])) for r in rows)) for c in COLUMNS}
    line = " | ".join(c.rjust(widths[c]) for c in COLUMNS)
    print(line, "-" * len(line), sep="\n")
    for r in rows:
        print(" | ".join(str(r[c]).rjust(widths[c]) for c in COLUMNS))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--stages", type=int, nargs="+", default=[1000, 10000, 50000])
    p.add_argument("--sensors", type=int, default=30)
    p.add_argument("--anomaly-rate", type=float, default=0.05)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--timeout", type=float, default=600)
    p.add_argument("--out", default=str(ROOT / "results" / "experiment_results.csv"))
    args = p.parse_args()

    # 측정 대상인 subscriber를 별도 프로세스로 실행 (실제 배포 형태와 동일)
    sub = subprocess.Popen([sys.executable, str(ROOT / "subscriber.py")], cwd=ROOT,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(3)
    rows = []
    try:
        for n in args.stages:
            print(f"stage {n:,} ...", flush=True)
            rows.append(run_stage(n, args))
    finally:
        time.sleep(1.5)  # 마지막 배치의 ACK 전송 대기 (미전송 시 다음 실행 때 브로커가 재전달)
        sub.terminate()

    print()
    print_table(rows)
    Path(args.out).parent.mkdir(exist_ok=True)
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)
    print(f"\nsaved: {args.out}")


if __name__ == "__main__":
    main()
