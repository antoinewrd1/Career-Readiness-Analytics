"""05 — Predictive modeling (local mirror).
Leakage-free target: overall self-assessed readiness ~ experience attributes.
MLflow logs locally to ./mlruns — inspect with `mlflow ui` then open the URL.
"""
import os
import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from pyspark.sql import functions as F
from sklearn.model_selection import train_test_split, KFold, cross_val_score
from sklearn.dummy import DummyRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import RidgeCV, LassoCV
from sklearn.ensemble import RandomForestRegressor
from sklearn.inspection import permutation_importance
from config import get_spark, load_table, EXPORT_DIR, ROOT, SEED

spark = get_spark()
mlflow.set_tracking_uri(f"file://{os.path.join(ROOT, 'mlruns')}")
mlflow.set_experiment("career-readiness-modeling")

# 1. modeling table: target + features, complete responders only
target = (load_table(spark, "gold_respondent_dimension")
          .filter("respondent_type = 'student'")
          .groupBy("respondent_id")
          .agg(F.avg("dim_score").alias("overall_readiness"),
               F.count("dimension").alias("n_dims_scored")))
pdf = (load_table(spark, "silver_student")
       .join(target, "respondent_id", "inner")
       .filter("n_dims_scored = 7")
       .toPandas())
print("modeling rows:", len(pdf))
pdf.to_csv(os.path.join(EXPORT_DIR, "modeling_table.csv"), index=False)
spark.stop()   # Spark's job is done; everything below is sklearn

# 2. feature matrix
onehot  = [c for c in pdf.columns if c.startswith(("type_", "status_"))]
numeric = ["exp_hours_per_week", "exp_weeks", "exp_paid", "exp_for_credit"]
X = pdf[numeric + onehot].copy()
for c in numeric:
    X[f"{c}_missing"] = X[c].isna().astype(int)
X["exp_hours_per_week"] = X["exp_hours_per_week"].fillna(X["exp_hours_per_week"].median())
X["exp_weeks"]          = X["exp_weeks"].fillna(X["exp_weeks"].median())
X["exp_paid"]           = X["exp_paid"].fillna(0)
X["exp_for_credit"]     = X["exp_for_credit"].fillna(0)
X = X.loc[:, X.std() > 0]
y = pdf["overall_readiness"].astype(float)
print(f"X: {X.shape} | y mean {y.mean():.3f} sd {y.std():.3f}")

# 3. split + evaluation harness
X_tr, X_te, y_tr, y_te = train_test_split(X, y, test_size=0.2, random_state=SEED)
cv = KFold(n_splits=5, shuffle=True, random_state=SEED)

def evaluate(model, name):
    with mlflow.start_run(run_name=name):
        model.fit(X_tr, y_tr)
        pred = model.predict(X_te)
        rmse = float(np.sqrt(mean_squared_error(y_te, pred)))
        mae, r2 = float(mean_absolute_error(y_te, pred)), float(r2_score(y_te, pred))
        mlflow.log_params({"model": name, "n_train": len(X_tr),
                           "n_test": len(X_te), "n_features": X.shape[1]})
        mlflow.log_metrics({"rmse": rmse, "mae": mae, "r2": r2})
        print(f"{name:>16} | RMSE {rmse:.3f} | MAE {mae:.3f} | R2 {r2:+.3f}")
        return model, r2

baseline, r2_b = evaluate(DummyRegressor(strategy="mean"), "baseline_mean")
ridge = Pipeline([("scale", StandardScaler()),
                  ("model", RidgeCV(alphas=np.logspace(-3, 3, 25), cv=cv))])
lasso = Pipeline([("scale", StandardScaler()),
                  ("model", LassoCV(alphas=np.logspace(-4, 1, 25), cv=cv,
                                    random_state=SEED, max_iter=20000))])
ridge, r2_r = evaluate(ridge, "ridge_cv")
lasso, r2_l = evaluate(lasso, "lasso_cv")
rf = RandomForestRegressor(n_estimators=500, max_depth=6, min_samples_leaf=10,
                           random_state=SEED, n_jobs=-1)
rf, r2_f = evaluate(rf, "random_forest")
cv_r2 = cross_val_score(rf, X_tr, y_tr, cv=cv, scoring="r2")
print(f"RF 5-fold CV R2 (train): {cv_r2.mean():+.3f} ± {cv_r2.std():.3f}")

# 4. interpretation
coefs = pd.Series(lasso.named_steps["model"].coef_, index=X.columns)
nz = coefs[coefs != 0].sort_values(key=abs, ascending=False)
print("\nLasso non-zero coefficients (standardized):")
print(nz.to_string() if len(nz) else "  (all zeroed — no linear signal)")

best = max([(r2_r, ridge), (r2_l, lasso), (r2_f, rf)], key=lambda t: t[0])[1]
pi = permutation_importance(best, X_te, y_te, n_repeats=30, random_state=SEED)
print("\nPermutation importance (best model, top 10):")
print(pd.Series(pi.importances_mean, index=X.columns)
        .sort_values(ascending=False).head(10).to_string())

print("\nDone. Inspect runs with:  mlflow ui   (from the project folder)")
