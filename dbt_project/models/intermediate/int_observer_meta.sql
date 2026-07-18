-- Replaces silver_observer: cleaned supervisory tenure with a banded category.

select
    respondent_id,
    supervisory_org,
    vp_org,
    try_cast(years_supervising_raw as double) as years_supervising,
    case
        when try_cast(years_supervising_raw as double) < 2  then '0-1'
        when try_cast(years_supervising_raw as double) < 5  then '2-4'
        when try_cast(years_supervising_raw as double) < 10 then '5-9'
        else '10+'
    end as tenure_band
from {{ ref('stg_observer') }}
