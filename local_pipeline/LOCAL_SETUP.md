# Local pipeline — setup & run

Mirrors the Databricks notebooks (Steps 1-6) on a laptop with local PySpark +
Delta Lake. Same medallion logic; tables saved by path under ./lakehouse.

## Prerequisites
- Python 3.10-3.12
- Java 11 or 17 (Spark needs a JVM): `java -version` to check.
  - Windows: run under WSL2 (recommended) or install Java + set JAVA_HOME.
- R + RStudio for the R half (scripts/r_analysis.R) - already set up.

## Setup
```bash
cd local_pipeline
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

Place the two survey CSVs in `data/raw/` named exactly:
- student.csv
- observer.csv

## Run (in order)
```bash
python 01_bronze.py     # ingest + profile
python 02_silver.py     # conform, dedup, unpivot, one-hot
python 03_gold.py       # aggregates, rankings, gap + CSV exports
python 04_viz.py        # matplotlib charts -> viz/
python 05_model.py      # sklearn models, MLflow -> ./mlruns
mlflow ui               # open the printed URL to browse runs
```

The R suite (ranking, ggplot2 charts, lm) reads from `data/exports/` -
point r_analysis.R's data paths there and run it in RStudio.

## Layout
```
local_pipeline/
├── config.py            # Spark+Delta session, path-based table helpers
├── 01_bronze.py ... 05_model.py
├── data/raw/            # input CSVs (git-ignored)
├── data/exports/        # Gold CSVs for R / Quarto / Power BI
├── lakehouse/           # Delta tables (git-ignored)
├── viz/                 # PNG outputs
└── mlruns/              # MLflow tracking (git-ignored)
```
