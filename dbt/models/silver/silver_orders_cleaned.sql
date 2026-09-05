{{ config(unique_key='order_id') }}

with deduped as (
    select
        *,
        row_number() over (partition by order_id order by created_at desc) as rn
    from {{ ref('stg_orders') }}
)

select
    order_id,
    user_id,
    order_date,
    amount,
    status,
    created_at
from deduped
where rn = 1
  and amount >= 0
