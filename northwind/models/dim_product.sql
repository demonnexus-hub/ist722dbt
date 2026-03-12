with stg_product as (select * from {{source("northwind","Products")}} )

select {{dbt_utils.generate_surrogate_key(['stg_product.productid'])}} as productkey,
    stg_product.*
from stg_product