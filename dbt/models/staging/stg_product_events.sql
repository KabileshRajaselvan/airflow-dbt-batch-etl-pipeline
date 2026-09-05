select
    event_id,
    user_id,
    product_id,
    event_type,
    event_time::timestamp as event_time,
    event_date::date as event_date,
    price::numeric(10, 2) as price
from {{ source('raw', 'product_events') }}
where event_id is not null
  and user_id is not null
  and event_time is not null
