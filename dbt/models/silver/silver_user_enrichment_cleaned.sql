{{ config(unique_key='user_id') }}

-- Keeps only each user's most recent enrichment record (the mock API
-- re-serves every user's current snapshot on every call, so "latest
-- as_of_date per user" is the deduplication key, not a raw row dedup).
with ranked as (
    select
        *,
        row_number() over (partition by user_id order by as_of_date desc, extracted_at desc) as rn
    from {{ ref('stg_user_enrichment') }}
)

select
    user_id,
    segment,
    ltv_score,
    acquisition_channel,
    as_of_date,
    extracted_at
from ranked
where rn = 1
