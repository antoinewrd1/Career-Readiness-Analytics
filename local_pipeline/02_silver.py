"""02 — Silver: conform, validate, dedup, unpivot to long, one-hot (local mirror).

Keeps the safe-rename layer from the Databricks version: score columns are
temporarily renamed s_000..s_021 so melt() never sees special characters, then
mapped back to readable 'Dimension: Competency' text.
"""
import re
from pyspark.sql import functions as F, Window
from config import (get_spark, save_table, load_table, score_columns, col_like,
                    SCALE_MIN, SCALE_MAX)

spark = get_spark()


def conform_and_validate(df, type_prefix, respondent_type):
    raw_s_cols = score_columns(df)

    # safe-rename ONLY the 22 score columns (ids used by melt are already safe)
    s_map = {f"s_{i:03d}": c for i, c in enumerate(raw_s_cols)}       # safe -> raw
    for safe, raw in s_map.items():
        df = df.withColumnRenamed(raw, safe)
    s_cols = list(s_map.keys())

    # (a) content hash over all non-metadata source columns
    src_cols = [c for c in df.columns if not c.startswith("_")]
    df = df.withColumn("_row_hash", F.sha2(
        F.concat_ws("||", *[F.coalesce(F.col(f"`{c}`").cast("string"), F.lit("")) for c in src_cols]),
        256))

    # (b) dedup: keep first ingest per identical submission
    w = Window.partitionBy("_row_hash").orderBy(F.col("_ingested_at").asc())
    df = df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    # (c) deterministic surrogate key
    w_id = Window.orderBy("_row_hash")
    df = df.withColumn("respondent_id",
        F.concat(F.lit(type_prefix + "_"),
                 F.lpad(F.row_number().over(w_id).cast("string"), 5, "0")))

    # (d) conform org columns (observer's VP column has a trailing period)
    org_c = next((c for c in df.columns if c.strip().rstrip(".") == "Supervisory Organization"), None)
    vp_c  = next((c for c in df.columns if c.strip().rstrip(".").startswith("VP/Supervisory Org")), None)
    assert org_c and vp_c, "org columns not found"
    df = (df.withColumn("supervisory_org", F.trim(F.col(f"`{org_c}`")))
            .withColumn("vp_org",          F.trim(F.col(f"`{vp_c}`"))))

    # (e) cast + validate scores (out-of-scale -> null)
    for c in s_cols:
        casted = F.col(c).cast("double")
        ok = casted.isNotNull() & (casted >= SCALE_MIN) & (casted <= SCALE_MAX)
        df = df.withColumn(c, F.when(ok, casted))

    df = df.withColumn("_respondent_type", F.lit(respondent_type))
    return df, s_cols, s_map


def to_long(df, s_cols, s_map):
    long = df.melt(
        ids=["respondent_id", "_respondent_type", "supervisory_org", "vp_org",
             "_batch_id", "_ingested_at"],
        values=s_cols,
        variableColumnName="competency_full", valueColumnName="score")

    mapping = F.create_map([F.lit(x) for kv in s_map.items() for x in kv])
    long = long.withColumn("competency_full", mapping[F.col("competency_full")])

    parts = F.split("competency_full", ": ", 2)
    return (long.withColumnRenamed("_respondent_type", "respondent_type")
        .withColumn("dimension",  F.trim(parts.getItem(0)))
        .withColumn("competency", F.trim(parts.getItem(1)))
        .withColumn("is_valid_score", F.col("score").isNotNull())
        .drop("competency_full"))


def yes_no(col):
    v = F.upper(F.trim(col))
    return (F.when(v.isin("YES", "Y", "TRUE", "1"), 1)
             .when(v.isin("NO", "N", "FALSE", "0"), 0)
             .otherwise(F.lit(None).cast("int")))


def one_hot(df, col_name, prefix):
    vals = sorted(str(r[0]).strip() for r in df.select(col_name).distinct().collect()
                  if r[0] is not None and str(r[0]).strip())
    for v in vals:
        safe = re.sub(r"[^0-9a-zA-Z]+", "_", v.lower()).strip("_")
        df = df.withColumn(f"{prefix}_{safe}",
                           F.when(F.trim(F.col(col_name)) == v, 1).otherwise(0))
    return df, vals


if __name__ == "__main__":
    bronze_student  = load_table(spark, "bronze_student")
    bronze_observer = load_table(spark, "bronze_observer")

    st, sc, sc_map = conform_and_validate(bronze_student,  "S", "student")
    ob, oc, oc_map = conform_and_validate(bronze_observer, "O", "observer")

    silver_scores = to_long(st, sc, sc_map).unionByName(to_long(ob, oc, oc_map))
    save_table(silver_scores, "silver_scores")
    print("silver_scores rows:", load_table(spark, "silver_scores").count())

    # student features (col_like works on ORIGINAL names, still present in st)
    paid_c, credit_c = col_like(st, "opportunity paid"), col_like(st, "for credit")
    type_c   = col_like(st, "best describes")
    hours_c, weeks_c = col_like(st, "hours per week"), col_like(st, "total number of weeks")
    status_c = col_like(st, "Are you")

    student = (st
        .withColumn("exp_paid",           yes_no(F.col(f"`{paid_c}`")))
        .withColumn("exp_for_credit",     yes_no(F.col(f"`{credit_c}`")))
        .withColumn("exp_hours_per_week", F.col(f"`{hours_c}`").cast("double"))
        .withColumn("exp_weeks",          F.col(f"`{weeks_c}`").cast("double"))
        .withColumn("exp_type",       F.trim(F.col(f"`{type_c}`")))
        .withColumn("student_status", F.trim(F.col(f"`{status_c}`"))))
    student, type_vals   = one_hot(student, "exp_type", "type")
    student, status_vals = one_hot(student, "student_status", "status")

    keep = (["respondent_id", "supervisory_org", "vp_org", "exp_paid",
             "exp_for_credit", "exp_hours_per_week", "exp_weeks",
             "exp_type", "student_status"]
            + [c for c in student.columns if c.startswith(("type_", "status_"))])
    save_table(student.select(*keep), "silver_student")

    years_c = col_like(ob, "years you have been supervising")
    observer = (ob
        .withColumn("years_supervising", F.col(f"`{years_c}`").cast("double"))
        .withColumn("tenure_band",
            F.when(F.col("years_supervising") < 2, "0-1")
             .when(F.col("years_supervising") < 5, "2-4")
             .when(F.col("years_supervising") < 10, "5-9").otherwise("10+"))
        .select("respondent_id", "supervisory_org", "vp_org",
                "years_supervising", "tenure_band"))
    save_table(observer, "silver_observer")

    # quality checks
    ss = load_table(spark, "silver_scores")
    print("dimensions:", ss.select("dimension").distinct().count(), "(expect 7)")
    print("competencies:", ss.select("competency").distinct().count(), "(expect 22)")
    ss.groupBy("respondent_type").agg(
        F.count("*").alias("rows"),
        F.sum(F.when(~F.col("is_valid_score"), 1).otherwise(0)).alias("invalid_or_null")
    ).show()
    spark.stop()
