with f_order_fulfillment as (
    select * from {{ ref('fact_order_fullfillment') }}
),
d_customer as (
    select * from {{ ref('dim_customer') }}
)
select f.*, d.customer_key, d.customer_email,d.customer_firstname,d.customer_lastname,d.customer_address,d.customer_city,d.customer_zip
from f_order_fulfillment f
 left join dim_customer d on  d.customer_key= f.customerkey