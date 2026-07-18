-- A singular test: a SQL query that should return ZERO rows if the pipeline
-- is healthy. dbt fails the test if it returns any rows.
--
-- This encodes the exact sanity check the original PySpark notebook printed
-- manually ("Distinct dimensions (expect 7)", "Distinct competencies (expect
-- 22)") as a real, automated, CI-able assertion instead of a human having to
-- read console output and notice if a number looks wrong.

with dimension_count as (
    select count(distinct dimension) as n from {{ ref('int_scores_long') }}
),
competency_count as (
    select count(distinct competency) as n from {{ ref('int_scores_long') }}
)

select 'wrong dimension count' as failure_reason, n from dimension_count where n != 7
union all
select 'wrong competency count' as failure_reason, n from competency_count where n != 22
