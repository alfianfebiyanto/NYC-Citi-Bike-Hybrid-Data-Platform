select *
from {{ ref('mart_customer_orders') }}
where lifetime_value < 0

