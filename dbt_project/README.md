# Career Readiness: dbt project

Refactors the Silver-to-Gold layer of the career readiness pipeline (originally
PySpark notebooks `02_silver_transform.py` and `03_gold_analytics.py`) into a
tested, documented dbt project. Bronze ingestion stays as PySpark, since
reading raw CSVs isn't a natural dbt task; everything from Silver onward is
pure SQL here.

## Why a separate schema

This project targets `workspace.career_readiness_dbt`, not the original
`workspace.career_readiness`. dbt reads the existing `bronze_student` /
`bronze_observer` tables as a **source** (untouched) and rebuilds everything
downstream into the new schema. This means you can validate dbt's output
against the original PySpark Gold tables before trusting it:

```sql
-- row counts should match
select count(*) from workspace.career_readiness.gold_perception_gap;
select count(*) from workspace.career_readiness_dbt.fct_perception_gap;

-- values should match to 3 decimal places
select * from workspace.career_readiness_dbt.fct_perception_gap order by dimension;
select * from workspace.career_readiness.gold_perception_gap order by dimension;
```

## Setup

1. Install: `pip install dbt-databricks` (recommend a virtual environment).
2. Copy `profiles.yml.example` to `~/.dbt/profiles.yml` and fill in your
   warehouse hostname, HTTP path, and personal access token (the same values
   used for the Power BI connection).
3. From this folder: `dbt debug` — confirms the connection before anything else.

## Run

```bash
dbt run          # builds every model, in dependency order, automatically
dbt test         # runs every generic + custom test
dbt docs generate && dbt docs serve   # interactive lineage graph in your browser
```

Run a single model and its dependencies: `dbt run --select +fct_perception_gap`

## Project layout

```
models/
├── sources.yml              # declares bronze_student / bronze_observer as sources
├── staging/                 # 1:1 with Bronze: dedup, surrogate keys, cleaning
│   ├── stg_student.sql
│   ├── stg_observer.sql
│   └── stg_schema.yml
├── intermediate/             # unpivot, one-hot encoding, tenure banding
│   ├── int_scores_long.sql   # replaces silver_scores (the keystone long table)
│   ├── int_student_features.sql   # replaces silver_student
│   ├── int_observer_meta.sql      # replaces silver_observer
│   └── int_schema.yml
└── marts/                    # the Gold-equivalent analytics layer
    ├── fct_respondent_dimension.sql
    ├── fct_competency_summary.sql
    ├── fct_dimension_summary.sql
    ├── fct_dimension_rank.sql     # SQL window functions: RANK/DENSE_RANK/ROW_NUMBER
    ├── fct_perception_gap.sql     # pooled-SD Cohen's d
    └── marts_schema.yml

macros/
├── unpivot_scores.sql        # generates the melt-equivalent UNION ALL via Jinja
└── one_hot_encode.sql        # discovers categories at compile time via run_query()

tests/
└── assert_dimension_and_competency_counts.sql   # custom singular test
```

## What changed versus the PySpark version

- The "safe-rename" workaround (renaming columns to `col_000` before melting)
  is gone. Backtick-quoted identifiers and Jinja string literals known at
  compile time make it unnecessary in SQL.
- The three-language ranking demo (SQL / PySpark / R) still exists in the
  original notebooks; `fct_dimension_rank.sql` here is the dbt-native version
  of the same window-function logic, now testable and documented.
- Tests replace manual "eyeball the printed output" quality checks with
  automated, CI-able assertions (`dbt test` exits non-zero on failure).
