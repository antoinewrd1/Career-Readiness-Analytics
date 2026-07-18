"""Simulated Kafka producer for the career readiness survey data.

IMPORTANT, this is a simulation, not a real streaming source. The
underlying data (student.csv, observer.csv) is a static, historical export
already validated through the rest of this project's pipeline. This script
replays each row as a Kafka event with a short delay between messages, to
practice producer mechanics (serialization, keys, partitioning, delivery
callbacks) against data whose correct shape and downstream results are
already known, useful for learning, not a genuine live source.
"""
import csv
import json
import random
import time
from pathlib import Path

from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092"

# Point these at your actual CSVs. Defaults assume this folder sits as a
# sibling of local_pipeline/ and reuses its raw data, adjust if yours live
# somewhere else.
STUDENT_CSV = Path("../local_pipeline/data/raw/student.csv")
OBSERVER_CSV = Path("../local_pipeline/data/raw/observer.csv")

TOPIC_STUDENT = "survey-student-submissions"
TOPIC_OBSERVER = "survey-observer-submissions"


def delivery_report(err, msg):
    if err is not None:
        print(f"  delivery failed: {err}")
    # Silent on success on purpose, hundreds of per-row confirmations would
    # drown out the progress prints below.


def stream_csv(producer: Producer, csv_path: Path, topic: str):
    if not csv_path.exists():
        raise FileNotFoundError(
            f"{csv_path} not found. Edit STUDENT_CSV / OBSERVER_CSV at the "
            f"top of this script to point at your real CSV locations."
        )

    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    print(f"Streaming {len(rows)} rows from {csv_path.name} -> topic '{topic}'")
    for i, row in enumerate(rows):
        payload = json.dumps(row).encode("utf-8")
        # Stand-in key since the raw CSV has no submission ID; a real source
        # system would key on its own submission/respondent ID instead.
        # Keying still matters here: it's what routes related messages to
        # the same partition, preserving per-key order.
        key = str(i).encode("utf-8")

        producer.produce(topic, key=key, value=payload, callback=delivery_report)
        producer.poll(0)  # serve delivery callbacks without blocking

        if i % 50 == 0:
            print(f"  ...{i}/{len(rows)} produced")

        # Simulated arrival spacing. Raise this range for a slower, more
        # visibly "live" demo in Kafka UI; lower it (or remove the sleep
        # entirely) once you've seen it work and just want speed.
        time.sleep(random.uniform(0.02, 0.08))

    producer.flush()
    print(f"Done: {csv_path.name} fully produced to '{topic}'.\n")


def main():
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    stream_csv(producer, STUDENT_CSV, TOPIC_STUDENT)
    stream_csv(producer, OBSERVER_CSV, TOPIC_OBSERVER)


if __name__ == "__main__":
    main()
