select
    user_id,
    segment,
    ltv_score::numeric(6, 2) as ltv_score,
    acquisition_channel,
    as_of_date::date as as_of_date,
    extracted_at::timestamp as extracted_at
from {{ source('raw', 'user_enrichment') }}
where user_id is not null
  and as_of_date is not null
