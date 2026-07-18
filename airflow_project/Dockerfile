# Extends the SAME base image the official docker-compose.yaml references,
# adding just the two packages this project needs: the Databricks provider
# (for DatabricksRunNowOperator) and dbt-databricks (so the container can run
# `dbt run` / `dbt test` directly via BashOperator).
#
# IMPORTANT: set AIRFLOW_BASE_IMAGE to match whatever tag the
# docker-compose.yaml you curled actually references (look for
# "image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:X.Y.Z}" near the top of that
# file, inside the x-airflow-common block) so this image stays in lockstep
# with the rest of the stack instead of silently drifting to a different
# Airflow version.
ARG AIRFLOW_BASE_IMAGE=apache/airflow:3.3.0
FROM ${AIRFLOW_BASE_IMAGE}

RUN pip install --no-cache-dir \
    apache-airflow-providers-databricks \
    dbt-databricks
