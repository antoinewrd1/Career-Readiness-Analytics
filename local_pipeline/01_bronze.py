"""01 — Bronze: faithful raw ingestion + profiling (local mirror)."""
import os
from datetime import datetime, timezone
from pyspark.sql import functions as F
from config import (get_spark, save_table, load_table, score_columns,
                    RAW_DIR, STUDENT_FILE, OBSERVER_FILE)

spark = get_spark()
BATCH_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ingest_bronze(file_name, table, respondent_type):
    src = os.path.join(RAW_DIR, file_name)
    assert os.path.exists(src), f"Missing {src} — put the CSV in data/raw/"

    df = (spark.read
          .option("header", "true").option("inferSchema", "false")
          .option("multiLine", "true").option("escape", '"')
          .csv(src)
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_respondent_type", F.lit(respondent_type))
          .withColumn("_ingested_at", F.current_timestamp())
          .withColumn("_batch_id", F.lit(BATCH_ID)))

    save_table(df, table, column_mapping=True)  # raw headers have spaces/&/:
    n = load_table(spark, table).count()
    print(f"[OK] {table}: {n} rows, {len(df.columns)} columns  <- {file_name}")
    return df


def profile_scores(df, label):
    cols = score_columns(df)
    print(f"\n=== {label}: {len(cols)} competency columns ===")
    casted = df.select([F.col(f"`{c}`").cast("double").alias(c) for c in cols])
    casted.summary("count", "min", "max", "mean", "stddev").show(truncate=12)
    df.select([
        F.count(F.when(F.col(f"`{c}`").isNull() | (F.trim(F.col(f"`{c}`")) == ""), c)).alias(c[:24])
        for c in cols
    ]).show(truncate=12)


if __name__ == "__main__":
    bs = ingest_bronze(STUDENT_FILE,  "bronze_student",  "student")
    bo = ingest_bronze(OBSERVER_FILE, "bronze_observer", "observer")
    profile_scores(bs, "STUDENT")
    profile_scores(bo, "OBSERVER")
    spark.stop()
