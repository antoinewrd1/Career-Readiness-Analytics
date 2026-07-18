# Career Readiness: Airflow orchestration

Orchestrates the full lakehouse pipeline: a Databricks Job (the four PySpark
notebooks, on serverless compute) followed by `dbt run` and `dbt test`
against the validated dbt project, with dbt's parity check confirming the
freshly-rebuilt SQL layer still matches the freshly-rebuilt PySpark layer.

This folder is a sibling of `dbt_project/` (not nested inside it), so the
relative volume mount below (`../dbt_project`) resolves correctly:

```
your-repo/
├── dbt_project/          <- already built and validated
└── airflow_project/       <- this folder
```

## 1. Create the Databricks Job (one-time, via the UI)

Airflow triggers an *existing* Databricks Job rather than redefining the
pipeline in Python (Databricks' own recommended pattern, since job runs
triggered this way show up in the normal Jobs UI, same as any other run).

1. Databricks workspace -> **Workflows** (left sidebar) -> **Create Job**.
2. **Task 1**: name `bronze_ingestion`, type Notebook, path to
   `01_bronze_ingestion`, compute: **Serverless**.
3. **Task 2**: name `silver_transform`, depends on Task 1, notebook
   `02_silver_transform`, Serverless.
4. **Task 3**: name `gold_analytics`, depends on Task 2, notebook
   `03_gold_analytics`, Serverless.
5. **Task 4**: name `visualization`, depends on Task 3, notebook
   `04_visualization`, Serverless.
6. Save. Open the job's page and note the **numeric Job ID** (visible in the
   URL, e.g. `.../jobs/123456789012345`) - you'll need it in step 4 below.
7. Optional but recommended: click **Run now** once to confirm the job
   itself works standalone before Airflow ever touches it.

## 2. Get the official Airflow Docker Compose file

```bash
mkdir airflow_project && cd airflow_project   # or your existing folder
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
```

This project's `Dockerfile`, `.env.example`, and `dags/` belong in this same
folder, alongside the file you just curled.

## 3. Point Compose at the custom image (one targeted edit)

Open the curled `docker-compose.yaml` and find the `x-airflow-common` block
near the top (every official version has had this block for years, it's
where all services share their base image, environment, and volumes).

Make three small additions inside that one block:

1. **Replace** the `image:` line:
   ```yaml
   # was: image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:X.Y.Z}
   build: .
   ```
   Note the tag on that original line, then set the SAME tag as
   `AIRFLOW_BASE_IMAGE` in this project's `Dockerfile` so the two stay in sync.

2. **Add** to the existing `environment:` section:
   ```yaml
   DBT_DATABRICKS_HOST: ${DBT_DATABRICKS_HOST}
   DBT_DATABRICKS_HTTP_PATH: ${DBT_DATABRICKS_HTTP_PATH}
   DBT_DATABRICKS_TOKEN: ${DBT_DATABRICKS_TOKEN}
   ```

3. **Add** to the existing `volumes:` section:
   ```yaml
   - ../dbt_project:/opt/dbt_project
   - ./dbt_profiles:/opt/dbt_profiles
   ```

That's it, everything else in the official file stays untouched.

## 4. Configure credentials and start

```bash
cp .env.example .env
# edit .env: real Databricks host / http_path / token (same as Power BI / dbt),
# and AIRFLOW_UID (run `id -u` in WSL2/Linux, paste the result)

docker compose up airflow-init      # one-time DB init + admin user creation
docker compose up -d                 # starts everything else
```

Browse to **http://localhost:8080** (login: `airflow` / `airflow`, the
Quick Start default, change this if you ever run this somewhere more
exposed than your own laptop).

Two things to set in the UI before triggering the DAG:

- **Admin -> Variables -> Add**: key `career_readiness_databricks_job_id`,
  value = the numeric Job ID from step 1.
- **Admin -> Connections -> Add**: Connection Id `databricks_default`,
  Connection Type `Databricks`, Host = your workspace hostname, Password =
  your PAT (the field the Databricks provider expects the token in; if the
  current UI labels this differently, check the Databricks provider's own
  connection docs, this is the one part most likely to shift between
  provider versions).

## 5. Run it

DAGs list -> `career_readiness_pipeline` -> unpause (toggle on the left) ->
trigger manually (play button) for the first run, or just wait for the
`@weekly` schedule. Click into the run to watch each of the three tasks
(`run_databricks_ingestion_pipeline`, `dbt_run`, `dbt_test`) move through
the graph view, with logs available per task.

## Why this design

- **DatabricksRunNowOperator over SubmitRunOperator**: Databricks explicitly
  recommends this, since it triggers an existing, versioned Job definition
  rather than redefining the pipeline's structure inside Airflow's Python
  code, one source of truth for what the pipeline actually does.
- **Serverless compute, no cluster spec in Airflow**: avoids managing
  cluster lifecycle from the orchestrator; the Job's own task-level compute
  setting (Serverless) handles that.
- **Two separate profiles.yml files**: your local one (real, hardcoded
  values, used by your own `dbt debug`/`dbt run` from the terminal) and this
  project's env_var-based one (used only inside the containers). Neither
  setup can break the other.
- **Stage ordering (Databricks Job -> dbt run -> dbt test)**: dbt's parity
  test compares against the original PySpark Gold tables, so those must be
  freshly rebuilt first, or the comparison would be against stale data.

## Not yet included

Simulated Kafka ingestion (a separate roadmap item) and a Power BI dataset
refresh task are natural next additions to this DAG, neither is wired in yet.
