"""04 — Visualization: matplotlib suite (local mirror).
The ggplot2 half runs via r_analysis.R against data/exports/.
"""
import os
import numpy as np
import matplotlib
matplotlib.use("Agg")           # headless save; remove to view interactively
import matplotlib.pyplot as plt
from config import get_spark, load_table, VIZ_DIR

spark = get_spark()
os.makedirs(VIZ_DIR, exist_ok=True)

C_STUDENT, C_OBSERVER = "#4C78A8", "#F58518"
C_OVER, C_UNDER       = "#E45756", "#54A24B"

dim  = load_table(spark, "gold_dimension_summary").toPandas()
gap  = load_table(spark, "gold_perception_gap").toPandas()
comp = load_table(spark, "gold_competency_summary").toPandas()

# 1. dimension means — self vs observer
piv = dim.pivot(index="dimension", columns="respondent_type", values="mean_score")
piv["avg"] = piv.mean(axis=1)
piv = piv.sort_values("avg")
y, h = np.arange(len(piv)), 0.38

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(y - h/2, piv["student"],  height=h, label="Student (self)", color=C_STUDENT)
ax.barh(y + h/2, piv["observer"], height=h, label="Observer",       color=C_OBSERVER)
ax.set_yticks(y); ax.set_yticklabels(piv.index)
ax.set_xlabel("Mean score (1-4)"); ax.set_xlim(0, 4)
ax.set_title("Career readiness by dimension: self vs observer")
ax.legend(loc="lower right"); ax.grid(axis="x", alpha=0.3)
plt.tight_layout()
fig.savefig(os.path.join(VIZ_DIR, "01_dimension_self_vs_observer.png"), dpi=150)
plt.close(fig)

# 2. diverging perception gap
g = gap.sort_values("gap")
colors = [C_OVER if v > 0 else C_UNDER for v in g["gap"]]
fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(g["dimension"], g["gap"], color=colors)
ax.axvline(0, color="black", lw=0.9)
for yi, v in enumerate(g["gap"]):
    ax.text(v + (0.01 if v >= 0 else -0.01), yi, f"{v:+.2f}",
            va="center", ha="left" if v >= 0 else "right", fontsize=9)
ax.set_xlabel("Gap (student mean - observer mean)")
ax.set_title("Perception gap by dimension")
ax.grid(axis="x", alpha=0.3)
plt.tight_layout()
fig.savefig(os.path.join(VIZ_DIR, "02_perception_gap.png"), dpi=150)
plt.close(fig)

# 3. competency heatmap
hm = comp.pivot_table(index="competency", columns="respondent_type",
                      values="mean_score").sort_index()
fig, ax = plt.subplots(figsize=(6, 11))
im = ax.imshow(hm.values, aspect="auto", cmap="RdYlGn", vmin=1, vmax=4)
ax.set_xticks(range(len(hm.columns))); ax.set_xticklabels(hm.columns)
ax.set_yticks(range(len(hm.index)));   ax.set_yticklabels(hm.index, fontsize=8)
for i in range(hm.shape[0]):
    for j in range(hm.shape[1]):
        v = hm.values[i, j]
        if not np.isnan(v):
            ax.text(j, i, f"{v:.2f}", ha="center", va="center", fontsize=7)
fig.colorbar(im, ax=ax, label="Mean score", shrink=0.6)
ax.set_title("Competency means")
plt.tight_layout()
fig.savefig(os.path.join(VIZ_DIR, "03_competency_heatmap.png"), dpi=150)
plt.close(fig)

print("saved 3 charts ->", VIZ_DIR)
spark.stop()
