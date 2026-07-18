# Career Readiness Analytics Platform

**An end-to-end lakehouse pipeline analyzing how students see their own career readiness, and how their supervisors see it differently.**

Built on Databricks with medallion architecture (Bronze to Silver to Gold), this project analyzes a dual perspective university survey in which **840 students** rated themselves on 22 career readiness competencies and **roughly 1,000 workplace supervisors** rated students on the identical 22. The centerpiece is a **perception gap analysis**: where student self assessment and supervisor assessment diverge, quantified with Cohen's d effect sizes, ranked, and translated into actionable recommendations for career development programming.

The same analysis is deliberately implemented across **SQL, PySpark, and R** to demonstrate cross platform fluency, and surfaced through **Python and R visualization suites**, an **interactive Power BI report**, a **Quarto debrief deck**, and a **two page executive summary document**.

![Perception gap by dimension](viz/02_perception_gap.png)

---

## Key findings

Every one of the seven NACE aligned dimensions shows the same pattern: **students rate themselves lower than their supervisors rate them.** Not overconfidence, a confidence gap, the opposite of what conventional wisdom about self assessment typically predicts.

| Dimension | Student mean | Supervisor mean | Gap | Cohen's d |
|---|---|---|---|---|
| Career & Self-Development | 2.92 | 3.16 | -0.24 | -0.29 |
| Communication | 3.21 | 3.38 | -0.17 | -0.22 |
| Professionalism | 3.41 | 3.52 | -0.11 | -0.13 |
| Technology | 3.37 | 3.48 | -0.11 | -0.13 |
| Teamwork | 3.36 | 3.46 | -0.09 | -0.12 |
| Critical Thinking | 3.22 | 3.30 | -0.08 | -0.09 |
| Leadership | 3.07 | 3.14 | -0.07 | -0.08 |

*(Scale: 1 to 4. All figures from `gold_perception_gap.csv` and `gold_dimension_summary.csv`.)*

- **Both groups agree on the top strength: Professionalism** (3.41 self, 3.52 supervisor). Agreement at the top indicates a coherent assessment.
- **Weakest dimensions diverge, and that divergence is the story.** Students rated themselves lowest on **Career & Self-Development** (2.92); supervisors rated students lowest on **Leadership** (3.14).
- **No gap reaches medium effect size** (the conventional |d| >= 0.5 threshold). Self and supervisor assessments are broadly calibrated. Gaps are monitor items rather than urgent misalignments, though their complete directional consistency across all seven dimensions (7 of 7 negative) is a meaningful pattern in its own right.
- **Opportunity structure does not explain readiness self-perception.** A leakage-aware predictive analysis (see Roadmap) found that experiential learning attributes, paid status, credit status, hours per week, duration, and opportunity type, explain only about 3 to 4 percent of variance in students' self-assessed readiness (best model R2 = 0.035 on held-out data; independently confirmed by an unpenalized R regression, Adjusted R2 = 0.045). Whatever drives readiness self-perception, it is not placement logistics, which reinforces the case for feedback and calibration interventions over structural ones.
- **Implications:** (1) **Leadership** is the primary skill development target, the weakest dimension by external assessment. (2) **Career & Self-Development** warrants a confidence and awareness intervention as much as a skill one: it is both students' lowest self rating and their largest under-estimate, suggesting students do not fully recognize competence their supervisors already observe. (3) The uniform under-rating pattern argues for building structured supervisor feedback into experiential learning so students calibrate upward over time.

---

## Why this design

| Decision | Rationale |
|---|---|
| Medallion architecture | Raw data preserved (Bronze), business logic centralized (Silver), analytics consumed only from Gold; Power BI and reporting never touch raw or intermediate tables |
| Long format Silver table | Unpivoting 22 wide competency columns into tidy rows makes every downstream aggregation, ranking, and gap calculation a single query instead of 22 times duplicated logic |
| Respondent level scoring | Dimension scores are averaged within respondent first, making the respondent (not the survey item) the unit of analysis, required for honest effect sizes |
| Rankings in 3 languages | The identical strongest to weakest ranking in SQL window functions, PySpark `Window`, and R `dplyr`, same semantics, three dialects |
| Cohen's d on the gap | Raw mean differences do not convey practical significance; standardized effect sizes separate "statistically visible" from "actually actionable" |
| Quarto for the debrief | One source document renders to both reveal.js slides and PowerPoint, and hosts R and Python in the same file via knitr and reticulate |

---

## The data

Two survey exports from a university career readiness assessment (raw data not included in this repository for privacy):

- **Student file** (840 responses): self ratings on 22 competencies plus experiential learning attributes (opportunity type, paid or unpaid, credit status, hours per week, duration)
- **Observer file** (roughly 1,000 responses): supervisor ratings of students on the same 22 competencies plus years of supervisory experience

The 22 competencies roll up to **7 dimensions** aligned to the NACE Career Readiness Competencies (7 of the 8 standard competencies; Equity & Inclusion was not surveyed):

Career & Self-Development, Communication, Critical Thinking, Leadership, Professionalism, Teamwork, Technology

Ratings use a **1 to 4 scale**. The comparison is **unpaired and population level** (student and supervisor respondent pools, not matched supervisor to student pairs) and cross sectional.

---

## Architecture

```
                    +---------------------------------------------------+
                    |                DATABRICKS LAKEHOUSE                |
                    |                                                    |
 CSV exports  --->  |  BRONZE            SILVER            GOLD         |
 (UC Volume / S3)   |  raw Delta tables  cleaned, typed,   aggregates,   |
                    |  + lineage         deduped, long-    rankings,     |
                    |  metadata          format, one-hot   perception    |
                    |                    encoded           gap          |
                    +----------+-----------------------------+----------+
                               |                              |
                               v                              v
                      Python / R visualization       Power BI report,
                      (matplotlib, ggplot2)          Quarto deck, and
                                                      Word summary
```

### Bronze: faithful ingestion (`notebooks/01_bronze_ingestion.py`)
- Both CSVs read with `inferSchema=false` (all strings, casting is Silver's job)
- Lineage metadata on every row: batch ID, ingestion timestamp, source file via Unity Catalog's `_metadata.file_path`
- Delta column mapping to preserve raw headers containing spaces, `&`, and `:`
- Profiling suite: score ranges, null patterns, categorical cardinalities

### Silver: business ready (`notebooks/02_silver_transform.py`)
- Deduplication via `row_number()` over a content hash window
- Deterministic surrogate keys (`S_00001`, `O_00042`) minted from content hashes
- Score validation: values outside the 1 to 4 scale nulled and flagged, never silently kept
- Safe rename layer: columns temporarily renamed to sanitized identifiers to work around special character constraints in `melt`, with a mapping back to the original readable headers
- Unpivot: 22 competency columns collapsed into a tidy long table; dimension and competency recovered by splitting headers on the first `": "`
- One hot encoding of experience categoricals as explicit 0/1 indicator columns
- Outputs: `silver_scores` (long), `silver_student`, `silver_observer`

### Gold: analytics (`notebooks/03_gold_analytics.py`)
- `gold_respondent_dimension`: per respondent dimension scores (the honest analytical unit)
- `gold_competency_summary` / `gold_dimension_summary`: mean, median, SD, n
- `gold_dimension_rank`: strongest to weakest, computed three ways (SQL window functions, PySpark `Window`, R `dplyr`)
- `gold_perception_gap`: student mean minus supervisor mean per dimension, with pooled SD Cohen's d and direction labels
- `gold_powerbi_flat`: denormalized serving table for BI
- Org overlap self check to determine whether an org level paired comparison is statistically available

### Visualization (`notebooks/04_visualization.py`)
Parallel implementations in matplotlib and ggplot2, consistent color semantics across both: dimension means (paired bars / dumbbell), the diverging perception gap chart, and a 22 competency heatmap.

### Power BI report
Three page interactive report connected to Gold via Databricks SQL warehouse (personal access token authentication, Import mode): Executive Summary, Dimension Deep-Dive, and Recommendations. 14 DAX measures including `CALCULATE` filter context control, `RANKX` dynamic ranking, `DIVIDE` safe division, and field value conditional formatting matching the Python and R color scheme. Synced slicers and cross page drill-through.

### Quarto debrief deck (`reports/debrief.qmd`)
A single source document rendered to both reveal.js (HTML slides) and PowerPoint, built on the knitr engine with R chunks doing the primary analysis and one Python chunk via `reticulate` fulfilling the cross language goal. Contents include the dumbbell comparison, the diverging gap chart, a live ranked table, the perception gap table, the 22 competency heatmap, a mean plus/minus one standard deviation spread chart (showing that individual variation exceeds the group level gap), an explanation of Cohen's d for a non technical audience, and recommendations.

### Executive summary report (`reports/Career_Readiness_Summary_Report.docx`)
A two page Word document (Calibri font throughout) summarizing the full analysis for stakeholders who want a print or email friendly artifact rather than slides or a dashboard. Four distinct visualization types: a dumbbell chart, a diverging bar chart, a parity scatter plot (student mean vs. supervisor mean against a diagonal reference line, visually confirming every dimension falls on the same side), and a complete results table for all seven dimensions.

---

## Repository structure

```
├── notebooks/
│   ├── 01_bronze_ingestion.py      # raw ingest and profiling
│   ├── 02_silver_transform.py      # clean, dedup, unpivot, one-hot
│   ├── 03_gold_analytics.py        # aggregates, rankings (SQL/PySpark/R), gap
│   ├── 04_visualization.py         # matplotlib + ggplot2 chart suites
│   └── 05_model.py                 # predictive modeling (built, not yet executed; see Roadmap)
├── local_pipeline/                 # local, non-Databricks mirror of the full pipeline
│   ├── config.py                  # local Spark + Delta session, path-based table helpers
│   ├── 01_bronze.py ... 05_model.py
│   ├── r_analysis.R               # R ranking, ggplot2 charts, lm companion model
│   ├── requirements.txt
│   └── LOCAL_SETUP.md             # environment setup, including known Windows/Spark caveats
├── dbt_project/                     # Silver-to-Gold refactored into dbt (validated against PySpark Gold)
│   ├── models/staging/             # 1:1 with Bronze: dedup, surrogate keys
│   ├── models/intermediate/        # unpivot, one-hot encoding
│   ├── models/marts/               # Gold-equivalent analytics (SQL window functions, Cohen's d)
│   ├── macros/                     # unpivot_scores.sql, one_hot_encode.sql
│   ├── tests/                      # including the automated PySpark-parity check
│   └── README.md                  # setup, run order, full validation writeup
├── airflow_project/                 # orchestration: Databricks Job -> dbt run -> dbt test (validated end to end)
├── kafka_project/                   # simulated ingestion: producer/consumer via Docker Kafka (validated lossless; see Roadmap for open data question)
│   ├── Dockerfile                  # extends the official Airflow image with Databricks + dbt-databricks
│   ├── dags/career_readiness_dag.py
│   ├── dbt_profiles/profiles.yml   # env_var-based, separate from the local hardcoded profiles.yml
│   ├── .env.example
│   └── README.md                  # Databricks Job creation, Compose setup, Connection/Variable config
├── reports/
│   ├── debrief.qmd                 # Quarto debrief deck source
│   └── Career_Readiness_Summary_Report.docx
├── viz/                             # exported PNG figures
├── powerbi/                         # Gold CSV exports + .pbix
└── README.md
```

---

## Tech stack

**Platform:** Databricks (serverless), Delta Lake, Unity Catalog, local PySpark and Delta for offline reproducibility, dbt-databricks, Apache Airflow (Dockerized: Databricks Job trigger, dbt run, dbt test, validated end to end), Apache Kafka (KRaft mode, simulated ingestion, validated lossless end to end, see Roadmap for an open data-currency question this surfaced)
**Languages:** Python (PySpark, pandas, matplotlib, scikit-learn), R (dplyr, tidyr, ggplot2, readr), SQL (Spark SQL), DAX
**Reporting:** Power BI Desktop (Databricks SQL warehouse connector, Import mode), Quarto (knitr engine with reticulate), Microsoft Word

---

## Roadmap

- [x] **Predictive modeling.** Executed end to end on Databricks serverless (Python/sklearn with MLflow tracking), with the R companion (`lm`) run locally in RStudio, after local Spark execution was abandoned due to environment obstacles (Java version, Windows `winutils`, disk space, documented in `local_pipeline/LOCAL_SETUP.md`). Design: leakage-aware (target and features from different survey sections, never predicting a composite from its own components), 833 complete responders, 24 features, 80/20 split with 5-fold CV, a mean-predictor baseline reported alongside every model. **Result: a clean null.** Experiential learning attributes (paid status, credit status, hours, duration, opportunity type) do not meaningfully predict self-assessed readiness: best model (Ridge) R2 = 0.035 on held-out data, Lasso 0.029, constrained Random Forest 0.020, RF cross-validation 0.005 +/- 0.083 (statistically indistinguishable from zero), and the Leadership secondary target R2 = 0.008. The R `lm` companion (Adjusted R2 = 0.045) independently agrees. The only faint association: paid experience correlates weakly positively with self-rated readiness (top of both Lasso coefficients and permutation importance), too small to treat as a driver, and confounded by self-selection. **Implication:** roughly 96 percent of variance in readiness self-perception lies outside opportunity structure, which supports the project's central recommendation that the intervention lever is supervisor feedback and calibration, not placement logistics.
- [x] **dbt.** Silver to Gold transformations refactored into a tested, documented dbt project (`dbt_project/`), reading the existing Bronze tables as a source and rebuilding Silver and Gold in pure SQL. Validated against the original PySpark Gold tables: all 7 dimensions match to full floating point precision on every statistic (means, standard deviations, gap, Cohen's d). Two real bugs were caught and fixed during validation, a column name mismatch and an incomplete deduplication key that silently dropped respondents, neither of which a clean `dbt run` or `dbt test` alone would have revealed. The comparison itself is now a permanent automated test (`assert_matches_original_gold_perception_gap.sql`) rather than a one time manual check.
- [x] **Airflow.** Orchestration layer built and validated end to end (`airflow_project/`): a custom Dockerfile extending the official Airflow image with the Databricks provider and dbt-databricks, running via Docker Compose (Postgres, Redis, scheduler, api-server, triggerer, worker). The DAG triggers a Databricks Job (the four PySpark notebooks on serverless compute) via `DatabricksRunNowOperator`, confirmed against a live workspace run, polled to completion in real time, followed by `dbt run` (all 10 models) and `dbt test` (the full suite, including the automated PySpark-parity check), staged in that order specifically so the parity test checks against freshly rebuilt Gold tables rather than a stale snapshot. Setup, including Databricks Job creation, Docker Compose configuration, and Connection and Variable setup, is documented in `airflow_project/README.md`. One real bug was caught and fixed during setup: a dropped Jinja brace in `fct_perception_gap.sql` (introduced while relocating project folders) that broke the containerized build while local copies were unaffected, a good example of why running the pipeline in a second, independent environment is worth doing even after local validation.
- [x] **Simulated Kafka ingestion.** Built and validated (`kafka_project/`): single-node Kafka in KRaft mode (no ZooKeeper) plus Kafka UI, run via Docker Compose. A producer replays each survey CSV row as a keyed event across 3 partitions per topic, with simulated arrival delay; a consumer uses a named consumer group (offset tracking, so restarts resume rather than reprocess) and flushes size-or-time micro-batches to Parquet in a `streamed_bronze/` folder, deliberately separate from the project's validated Bronze Delta tables so this practice exercise can never affect real results. Verified lossless by direct reconciliation: every row produced was independently confirmed landed, with zero loss or duplication introduced by Kafka itself (student: 855 source rows, 855 landed; observer: 8,338 source rows, 8,338 landed). Still labeled a simulation: the source is static historical survey data, not a live system.

  **Open question surfaced during this reconciliation, not yet resolved:** the observer source file's row count (8,338, confirmed against the same file on the Databricks Volume) is substantially higher than every `observer_n` figure reported in this project's validated Key Findings, Gold layer, dbt parity test, and Power BI report (962 to 1,003 across all 7 dimensions). Removing exact full-row duplicates (5,663 found) leaves 2,675 unique rows, still roughly 2.7 times the validated figures. The leading hypothesis is that the source survey remained open for continued response collection after the original analysis was performed, meaning the Volume file may simply be larger now than the snapshot the rest of this project was built and validated against, this is unconfirmed. The Key Findings section above reflects the originally validated snapshot; it has not been rerun against the current, larger file. See Notes on methodology.

---

## Notes on methodology

- **Unit of analysis:** dimension scores are computed per respondent before cross respondent aggregation, so effect sizes reflect independent people rather than inflated item counts.
- **Gap direction:** positive gap means students rate themselves higher than supervisors do (a calibration blind spot); negative means students underrate themselves (a confidence and visibility gap). This dataset produced uniformly negative gaps across all seven dimensions.
- **Unpaired design:** student and supervisor means come from two respondent pools, not matched supervisor to student pairs; "under-rating" describes group level divergence, not individual miscalibration. Findings are cross sectional associations, not causal claims.
- **Small effect honesty:** no dimension reached |d| >= 0.5; the report presents gaps as monitor items rather than overstating their practical significance.
- **Source data currency (open question):** the Key Findings above reflect a specific validated snapshot of the survey data. A later reconciliation during the Kafka exercise (see Roadmap) found the current observer source file on the Databricks Volume contains substantially more rows (8,338, versus the 962 to 1,003 range validated in every downstream table) than the snapshot this analysis was built against. This has not been investigated further or resolved; the Key Findings have not been rerun against the larger file, and no claim is made here about whether the reported gaps and effect sizes would hold on the current, larger dataset.
- **Data privacy:** raw survey responses and organizational identifiers are not committed to this repository; only aggregate statistics and code are published.
