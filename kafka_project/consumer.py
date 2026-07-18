"""Simulated Kafka consumer: subscribes to both survey topics, batches
incoming messages, and lands them as Parquet files, a lightweight stand-in
Bronze layer for this practice exercise.

Deliberately kept separate from the project's validated Bronze Delta tables:
this writes to ./streamed_bronze/ only, so nothing here can ever collide
with or corrupt the real, already-validated pipeline results.

Run this BEFORE or WHILE producer.py runs. A consumer group only sees
messages from where it last left off; with auto.offset.reset="earliest" a
brand-new group (first-ever run) reads from the very beginning of each
topic, so running the consumer after the producer has already finished
still picks up everything.
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer

BOOTSTRAP_SERVERS = "localhost:9092"

# Consumer group: Kafka tracks this group's committed offset per partition,
# so stopping and restarting this script resumes where it left off instead
# of reprocessing everything (or, with a NEW group name, starting over).
GROUP_ID = "career-readiness-bronze-loader"

TOPICS = ["survey-student-submissions", "survey-observer-submissions"]

OUTPUT_DIR = Path("streamed_bronze")
BATCH_SIZE = 25  # flush after this many messages...
BATCH_TIMEOUT_SEC = 5.0  # ...or after this many seconds, whichever comes first
# This size/time dual trigger is a standard micro-batching pattern: it caps
# both how much can accumulate in memory and how long data can sit unflushed
# during a quiet period.


def flush_batch(topic: str, rows: list):
    if not rows:
        return
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    safe_topic = topic.replace("survey-", "").replace("-submissions", "")
    out_path = OUTPUT_DIR / f"{safe_topic}_{ts}.parquet"
    pd.DataFrame(rows).to_parquet(out_path, index=False)
    print(f"  flushed {len(rows)} rows -> {out_path}")


def main():
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(TOPICS)

    buffers = {t: [] for t in TOPICS}
    last_flush = {t: time.time() for t in TOPICS}
    total = {t: 0 for t in TOPICS}

    print(f"Consuming from {TOPICS} as group '{GROUP_ID}'. Ctrl+C to stop.")
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Nothing arrived this second; still check the time-based
                # flush trigger so data doesn't sit unflushed indefinitely
                # during a lull.
                for t in TOPICS:
                    if buffers[t] and (time.time() - last_flush[t] > BATCH_TIMEOUT_SEC):
                        flush_batch(t, buffers[t])
                        total[t] += len(buffers[t])
                        buffers[t] = []
                        last_flush[t] = time.time()
                continue
            if msg.error():
                print(f"  consumer error: {msg.error()}")
                continue

            topic = msg.topic()
            row = json.loads(msg.value().decode("utf-8"))
            buffers[topic].append(row)

            if len(buffers[topic]) >= BATCH_SIZE:
                flush_batch(topic, buffers[topic])
                total[topic] += len(buffers[topic])
                buffers[topic] = []
                last_flush[topic] = time.time()

    except KeyboardInterrupt:
        print("\nStopping, flushing any remaining buffered rows...")
        for t in TOPICS:
            if buffers[t]:
                flush_batch(t, buffers[t])
                total[t] += len(buffers[t])
        print("Totals landed this run:", total)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
