# Career Readiness: simulated Kafka ingestion

## What this is, and isn't

This replays the career readiness survey CSVs (student and observer
responses) through a real, locally running Kafka broker, one message per
row, with a short delay between messages, to practice producer/consumer
mechanics: serialization, keys and partitioning, consumer groups and offset
tracking, and micro-batch landing.

**This is a simulation, not a genuine streaming source.** The underlying
data is a static historical export, already ingested, validated, and
analyzed through the rest of this project (Bronze through Gold, dbt,
Airflow). Nothing here replaces that pipeline or feeds back into it, the
consumer lands data into a clearly separate `streamed_bronze/` folder as
Parquet files, deliberately kept apart from the project's validated Bronze
Delta tables so this practice exercise can never collide with or corrupt
real results.

## Folder placement

This folder should sit as a sibling of `local_pipeline/`, `dbt_project/`,
and `airflow_project/`, in the one canonical project location (not a new
duplicate tree):

```
your-project-folder/
├── local_pipeline/
├── dbt_project/
├── airflow_project/
└── kafka_project/          <- this folder
```

`producer.py` defaults to reading CSVs from `../local_pipeline/data/raw/`,
adjust the two paths at the top of that file if yours live elsewhere.

## Setup

```powershell
docker compose up -d
```

Give it 15 to 30 seconds. Kafka's healthcheck needs a moment to pass, check
before moving on:

```powershell
docker compose ps
```

Both `kafka` and `kafka-ui` should show `Up (healthy)`.

Then, in a Python virtual environment (recommended, keeps this isolated
from other projects' dependencies):

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Run

**1. Create the topics** (once):
```powershell
python create_topics.py
```

**2. Start the consumer** in one terminal (leave it running):
```powershell
python consumer.py
```

**3. Start the producer** in a second terminal:
```powershell
python producer.py
```

Watch the consumer's terminal print `flushed N rows -> ...` as batches land.
Check `streamed_bronze/` for the output Parquet files. Stop the consumer
with Ctrl+C when the producer finishes, it flushes any remaining buffered
rows before exiting.

**Optional: watch it visually.** Browse to `http://localhost:8090` (Kafka
UI) while the producer is running, you can see both topics, their
partitions, and messages arriving in near real time.

## What this demonstrates

- **Keys and partitioning**: each message is keyed, and both topics are
  created with 3 partitions (`create_topics.py`), rather than relying on
  Kafka's single-partition auto-creation default.
- **Consumer groups**: `consumer.py` uses a named group
  (`career-readiness-bronze-loader`), Kafka tracks its committed offset per
  partition, so stopping and restarting the script resumes rather than
  reprocessing everything.
- **Micro-batching**: the consumer flushes on whichever comes first, a
  message-count threshold or a time threshold, a standard streaming
  ingestion pattern that bounds both memory use and staleness.
- **Delivery callbacks**: the producer confirms each message actually
  reached the broker rather than assuming a successful `produce()` call
  guarantees delivery.

## Known gotchas (from this project's own setup history)

- **`KAFKA_ADVERTISED_LISTENERS` mismatches** are the most common cause of
  "it connects once, then hangs or errors." The compose file's comments
  explain the three-listener setup; if producer/consumer can list topics
  but can't actually send or receive messages, this is the first thing to
  check.
- **Docker Desktop must actually be running** before `docker compose up`,
  and give the containers a real 15 to 30 seconds before running any Python
  script against them, connecting too early looks like a Kafka problem but
  is usually just a timing issue.
- **`confluent-kafka` should install from a prebuilt wheel** on Windows; if
  pip instead tries to compile from source and fails, that's worth a second
  look at your Python version rather than assuming Kafka itself is broken.

## Not included (possible follow-ups, not required)

- No schema registry, message formats here are plain JSON, not Avro or
  Protobuf.
- No exactly-once delivery semantics, this uses default at-least-once
  guarantees.
- The landing zone is Parquet, not Delta. A natural next step, if the local
  PySpark environment (`local_pipeline/`) is ever fully working, would be a
  small script or Spark Structured Streaming job reading these Parquet
  files (or directly from Kafka) into a real Bronze Delta table, closer
  parity with genuine streaming ingestion into the lakehouse. Not built
  here, since it would tie this exercise's reliability to that more fragile
  local Spark setup.
