{{ config(unique_key='event_id') }}

with deduped as (
    select
        *,
        row_number() over (partition by event_id order by event_time desc) as rn
    from {{ ref('stg_product_events') }}
)

select
    event_id,
    user_id,
    product_id,
    event_type,
    event_time,
    event_date,
    price
from deduped
where rn = 1
  and price >= 0
