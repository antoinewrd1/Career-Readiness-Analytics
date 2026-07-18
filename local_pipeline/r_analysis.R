# =============================================================================
# Career Readiness Analytics — R analysis suite (local)
# =============================================================================
# Serverless Databricks compute does not support R, so the project's three R
# components run locally against CSV exports of the Gold tables.
#
# Setup:
#   1. Create ./data and place in it (downloaded from the Databricks Volume):
#        - gold_dimension_summary.csv
#        - gold_perception_gap.csv
#        - modeling_table.csv
#   2. In RStudio: Session > Set Working Directory > To Source File Location
#   3. Run the whole script (Ctrl+Shift+Enter or source())
#
# Outputs: console tables + ./viz/04_dumbbell_R.png, ./viz/05_gap_R.png
# =============================================================================

# ---- 0. Packages -------------------------------------------------------------
for (p in c("readr", "dplyr", "tidyr", "ggplot2")) {
  if (!require(p, character.only = TRUE)) {
    install.packages(p)
    library(p, character.only = TRUE)
  }
}
dir.create("viz", showWarnings = FALSE)

# ---- 1. Ranking — dplyr window functions (mirrors SQL / PySpark) -------------
# min_rank = SQL RANK, dense_rank = DENSE_RANK, row_number = ROW_NUMBER
dim_summary <- readr::read_csv("data/gold_dimension_summary.csv",
                               show_col_types = FALSE)

ranked_r <- dim_summary %>%
  group_by(respondent_type) %>%
  mutate(rank_strongest = min_rank(desc(mean_score)),
         dense_rank     = dense_rank(desc(mean_score)),
         row_num        = row_number(desc(mean_score))) %>%
  arrange(respondent_type, rank_strongest) %>%
  ungroup()

cat("\n=== Dimension ranking (should match SQL & PySpark exactly) ===\n")
print(as.data.frame(ranked_r))

# ---- 2. Dumbbell chart — self vs observer by dimension -----------------------
wide <- dim_summary %>%
  select(dimension, respondent_type, mean_score) %>%
  tidyr::pivot_wider(names_from = respondent_type, values_from = mean_score) %>%
  mutate(dimension = reorder(dimension, (student + observer) / 2))

p1 <- ggplot(wide) +
  geom_segment(aes(x = observer, xend = student, y = dimension, yend = dimension),
               color = "grey70", linewidth = 1) +
  geom_point(aes(x = student,  y = dimension, color = "Student (self)"), size = 4) +
  geom_point(aes(x = observer, y = dimension, color = "Observer"),       size = 4) +
  scale_color_manual(values = c("Student (self)" = "#4C78A8",
                                "Observer"       = "#F58518")) +
  labs(title = "Self vs observer by dimension",
       x = "Mean score (1\u20134 scale)", y = NULL, color = NULL) +
  theme_minimal(base_size = 13)

ggsave("viz/04_dumbbell_R.png", p1, width = 9, height = 6, dpi = 150)
print(p1)

# ---- 3. Diverging perception-gap chart ---------------------------------------
gap <- readr::read_csv("data/gold_perception_gap.csv",
                       show_col_types = FALSE) %>%
  mutate(dimension = reorder(dimension, gap),
         dir = ifelse(gap > 0, "student over-rates", "student under-rates"))

p2 <- ggplot(gap, aes(x = gap, y = dimension, fill = dir)) +
  geom_col() +
  geom_vline(xintercept = 0) +
  scale_fill_manual(values = c("student over-rates"  = "#E45756",
                               "student under-rates" = "#54A24B")) +
  labs(title = "Perception gap by dimension",
       x = "Gap (student \u2212 observer)", y = NULL, fill = NULL) +
  theme_minimal(base_size = 13)

ggsave("viz/05_gap_R.png", p2, width = 9, height = 6, dpi = 150)
print(p2)

# ---- 4. Linear model companion (mirrors the Python Lasso story) ---------------
d <- readr::read_csv("data/modeling_table.csv", show_col_types = FALSE)

feat <- d %>%
  select(starts_with("type_"), starts_with("status_"),
         exp_paid, exp_for_credit, exp_hours_per_week, exp_weeks) %>%
  mutate(across(everything(), ~ ifelse(is.na(.), median(., na.rm = TRUE), .)))

fit <- lm(d$overall_readiness ~ ., data = feat)

cat("\n=== Linear model: overall_readiness ~ experience attributes ===\n")
print(summary(fit))
cat("\nAdjusted R^2:", round(summary(fit)$adj.r.squared, 4), "\n")
cat("(Compare sign/magnitude of significant coefficients against the\n",
    "Python Lasso's non-zero coefficients; NA rows = collinear one-hot\n",
    "levels R dropped automatically — expected, ignore.)\n")
