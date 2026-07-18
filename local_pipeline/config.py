"""Shared configuration for the local Career Readiness pipeline.

Local adaptations vs the Databricks notebooks:
- Delta tables are saved by PATH under ./lakehouse (no Unity Catalog locally);
  save_table()/load_table() replace saveAsTable()/spark.table().
- No dbutils / display(); use df.show() or .toPandas().
"""
import os
from pyspark.sql import SparkSession, functions as F

ROOT       = os.path.dirname(os.path.abspath(__file__))
RAW_DIR    = os.path.join(ROOT, "data", "raw")      # put the two CSVs here
EXPORT_DIR = os.path.join(ROOT, "data", "exports")  # CSVs for R / Quarto / Power BI
LAKE_DIR   = os.path.join(ROOT, "lakehouse")        # Delta tables live here
VIZ_DIR    = os.path.join(ROOT, "viz")

STUDENT_FILE  = "student.csv"    # rename your files to these (clean names, no spaces)
OBSERVER_FILE = "observer.csv"

SCALE_MIN, SCALE_MAX = 1.0, 4.0
SEED = 42

DIMENSION_PREFIXES = [
    "Career & Self-Development:", "Communication:", "Critical Thinking:",
    "Leadership:", "Professionalism:", "Teamwork:", "Technology:",
]


def get_spark(app="career-readiness-local"):
    """Local SparkSession with Delta Lake enabled."""
    from delta import configure_spark_with_delta_pip
    builder = (
        SparkSession.builder.appName(app)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")   # laptop-sized
        .config("spark.driver.memory", "4g")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def table_path(name: str) -> str:
    return os.path.join(LAKE_DIR, name)


def save_table(df, name: str, column_mapping: bool = False):
    """Path-based Delta write (replaces saveAsTable)."""
    w = (df.write.format("delta").mode("overwrite")
           .option("overwriteSchema", "true"))
    if column_mapping:  # needed when column names contain spaces / & / :
        w = (w.option("delta.columnMapping.mode", "name")
              .option("delta.minReaderVersion", "2")
              .option("delta.minWriterVersion", "5"))
    w.save(table_path(name))
    print(f"[saved] {name} -> {table_path(name)}")


def load_table(spark, name: str):
    """Path-based Delta read (replaces spark.table)."""
    return spark.read.format("delta").load(table_path(name))


def score_columns(df):
    return [c for c in df.columns
            if any(c.strip().startswith(p) for p in DIMENSION_PREFIXES)]


def col_like(df, needle):
    return next((c for c in df.columns if needle.lower() in c.lower()), None)
