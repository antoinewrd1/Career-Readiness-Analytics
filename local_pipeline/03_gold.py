"""03 — Gold: aggregations, rankings (SQL + PySpark), perception gap (local mirror).
Also exports the CSVs consumed by the local R script and the Quarto deck.
"""
import os
from pyspark.sql import functions as F, Window
from config import get_spark, save_table, load_table, EXPORT_DIR, SCALE_MAX

spark = get_spark()
os.makedirs(EXPORT_DIR, exist_ok=True)

scores = load_table(spark, "silver_scores").filter("is_valid_score")

# 1. respondent-level dimension scores (the honest analytical unit)
grd = (scores.groupBy("respondent_id", "respondent_type", "supervisory_org",
                      "vp_org", "dimension")
             .agg(F.avg("score").alias("dim_score")))
save_table(grd, "gold_respondent_dimension")

# 2. summaries
gcs = (scores.groupBy("respondent_type", "dimension", "competency")
       .agg(F.count("*").alias("n"),
            F.round(F.avg("score"), 3).alias("mean_score"),
            F.round(F.stddev("score"), 3).alias("sd_score"),
            F.expr("percentile_approx(score, 0.5)").alias("median_score"),
            F.min("score").alias("min_score"), F.max("score").alias("max_score")))
save_table(gcs, "gold_competency_summary")

gds = (grd.groupBy("respondent_type", "dimension")
       .agg(F.count("*").alias("n_respondents"),
            F.round(F.avg("dim_score"), 3).alias("mean_score"),
            F.round(F.stddev("dim_score"), 3).alias("sd_score"),
            F.round(F.expr("percentile_approx(dim_score, 0.5)"), 3).alias("median_score"))
       .withColumn("pct_of_max", F.round(F.col("mean_score") / SCALE_MAX * 100, 1)))
save_table(gds, "gold_dimension_summary")

# 3a. ranking — SQL window functions (temp view replaces the UC table reference)
gds.createOrReplaceTempView("gold_dimension_summary")
rank = spark.sql("""
    SELECT respondent_type, dimension, mean_score,
           RANK()       OVER (PARTITION BY respondent_type ORDER BY mean_score DESC) AS rank_strongest,
           DENSE_RANK() OVER (PARTITION BY respondent_type ORDER BY mean_score DESC) AS dense_rank,
           ROW_NUMBER() OVER (PARTITION BY respondent_type ORDER BY mean_score DESC) AS row_num
    FROM gold_dimension_summary
    ORDER BY respondent_type, rank_strongest
""")
save_table(rank, "gold_dimension_rank")
rank.show(14, truncate=False)

# 3b. ranking — PySpark Window (same result, DataFrame API)
w = Window.partitionBy("respondent_type").orderBy(F.desc("mean_score"))
(gds.withColumn("rank_strongest", F.rank().over(w))
    .withColumn("dense_rank",     F.dense_rank().over(w))
    .withColumn("row_num",        F.row_number().over(w))
    .orderBy("respondent_type", "rank_strongest")
).show(14, truncate=False)
# (3c — the R version — runs locally via r_analysis.R against the CSV exports below)

# 4. perception gap with Cohen's d
s = (gds.filter("respondent_type = 'student'")
     .select("dimension", F.col("mean_score").alias("student_mean"),
             F.col("sd_score").alias("student_sd"),
             F.col("n_respondents").alias("student_n")))
o = (gds.filter("respondent_type = 'observer'")
     .select("dimension", F.col("mean_score").alias("observer_mean"),
             F.col("sd_score").alias("observer_sd"),
             F.col("n_respondents").alias("observer_n")))

gap = (s.join(o, "dimension")
    .withColumn("gap", F.round(F.col("student_mean") - F.col("observer_mean"), 3))
    .withColumn("pooled_sd", F.sqrt(
        ((F.col("student_n") - 1) * F.col("student_sd")**2 +
         (F.col("observer_n") - 1) * F.col("observer_sd")**2) /
        (F.col("student_n") + F.col("observer_n") - 2)))
    .withColumn("cohens_d", F.round(F.col("gap") / F.col("pooled_sd"), 3))
    .withColumn("direction", F.when(F.col("gap") > 0, "student over-rates")
                              .when(F.col("gap") < 0, "student under-rates")
                              .otherwise("aligned"))
    .orderBy(F.desc(F.abs(F.col("gap")))))
save_table(gap, "gold_perception_gap")
gap.show(truncate=False)

# 5. exports for R / Quarto / Power BI fallback
for name in ["gold_dimension_summary", "gold_perception_gap",
             "gold_competency_summary", "gold_dimension_rank"]:
    load_table(spark, name).toPandas().to_csv(
        os.path.join(EXPORT_DIR, f"{name}.csv"), index=False)
    print("[exported]", name)

spark.stop()
