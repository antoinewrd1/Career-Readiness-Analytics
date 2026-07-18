-- Replaces gold_dimension_rank. Identical window-function logic to the
-- original notebook's SQL cell; now it's a dbt model instead of an ad hoc
-- spark.sql() call, so it's testable, documented, and versioned like
-- everything else in the project.

select
    respondent_type,
    dimension,
    mean_score,
    rank()       over (partition by respondent_type order by mean_score desc) as rank_strongest,
    dense_rank() over (partition by respondent_type order by mean_score desc) as dense_rank,
    row_number() over (partition by respondent_type order by mean_score desc) as row_num
from {{ ref('fct_dimension_summary') }}
