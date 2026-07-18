"""Career Readiness pipeline orchestration.

Stage 1 - run_databricks_ingestion_pipeline:
    Triggers a pre-existing Databricks Job (created once via the Databricks
    UI, see README) that runs the full PySpark pipeline in order:
    01_bronze_ingestion -> 02_silver_transform -> 03_gold_analytics ->
    04_visualization, all on serverless compute.

Stage 2 - dbt_run:
    Rebuilds the validated SQL refactor (Silver -> Gold, in the
    career_readiness_dbt schema) from the Bronze data Stage 1 just produced.

Stage 3 - dbt_test:
    Runs all 22 tests, including the automated parity check
    (assert_matches_original_gold_perception_gap) against the ORIGINAL Gold
    tables Stage 1 just refreshed.

Ordering matters: the parity test in Stage 3 compares dbt's output against
the original PySpark gold_perception_gap table. If Stage 1 didn't run first,
that comparison would be checking against a stale snapshot rather than data
from this pipeline run, a false pass (or a confusing false fail) either way.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

DATABRICKS_CONN_ID = "databricks_default"

# Set via Airflow UI: Admin -> Variables -> add
# "career_readiness_databricks_job_id" = <the numeric job id from Databricks
# Workflows, visible in the job's URL after you create it (see README)>.
DATABRICKS_JOB_ID = "{{ var.value.career_readiness_databricks_job_id }}"

DBT_PROJECT_DIR = "/opt/dbt_project"
DBT_PROFILES_DIR = "/opt/dbt_profiles"

default_args = {
    "owner": "antoine",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="career_readiness_pipeline",
    description=(
        "Orchestrates the career readiness lakehouse: Databricks ingestion "
        "job, then dbt Silver-to-Gold rebuild and validation."
    ),
    default_args=default_args,
    schedule="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["career-readiness", "databricks", "dbt"],
) as dag:

    run_databricks_pipeline = DatabricksRunNowOperator(
        task_id="run_databricks_ingestion_pipeline",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_id=DATABRICKS_JOB_ID,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run "
                      f"--project-dir {DBT_PROJECT_DIR} "
                      f"--profiles-dir {DBT_PROFILES_DIR}",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test "
                      f"--project-dir {DBT_PROJECT_DIR} "
                      f"--profiles-dir {DBT_PROFILES_DIR}",
    )

    run_databricks_pipeline >> dbt_run >> dbt_test
