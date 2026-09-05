select
    order_id,
    user_id,
    order_date::date as order_date,
    amount::numeric(10, 2) as amount,
    status,
    created_at::timestamp as created_at
from {{ source('raw', 'orders') }}
where order_id is not null
  and user_id is not null
  and order_date is not null
