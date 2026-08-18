with stg_orders as 
(
    select
        order_id as orderid,  
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }} as customerkey, 
        replace(to_date(TO_TIMESTAMP(order_date/1000000))::varchar,'-','')::int as orderdatekey,
        replace(to_date(TO_TIMESTAMP(shipped_date/1000000))::varchar,'-','')::int as shippeddatekey,
        replace(dateadd(day,3,to_date(TO_TIMESTAMP(shipped_date/1000000)))::varchar,'-','')::int as requireddatekey,
        'MART' as ordertype,
        to_date(TO_TIMESTAMP(order_date/1000000)) - to_date(TO_TIMESTAMP(shipped_date/1000000)) as daysfromordertoshipped,
        to_date(TO_TIMESTAMP(shipped_date/1000000)) - dateadd(day,3,to_date(TO_TIMESTAMP(shipped_date/1000000))) as shippedtorequireddelta,
        shippedtorequireddelta::int > 0 as shippedontime
    from {{source('fudgemart_V3','fm_orders')}}
),
stg_order_details as
(
    select 
        order_id as orderid,
        sum(order_qty) as quantity, 
    from {{source('fudgemart_V3','fm_order_details')}}
    group by orderid
),
stg_account_titles as 
(
    select
        at_id+100000 as orderid,  
        {{ dbt_utils.generate_surrogate_key(['at_account_id']) }} as customerkey, 
        replace(to_date(TO_TIMESTAMP(at_queue_date/1000000))::varchar,'-','')::int as orderdatekey,
        replace(to_date(TO_TIMESTAMP(at_shipped_date/1000000))::varchar,'-','')::int as shippeddatekey,
        replace(dateadd(day,2,to_date(TO_TIMESTAMP(at_shipped_date/1000000)))::varchar,'-','')::int as requireddatekey,
        'VIDEO' as ordertype,
        1 as quantity,
        to_date(TO_TIMESTAMP(at_queue_date/1000000)) - to_date(TO_TIMESTAMP(at_shipped_date/1000000)) as daysfromordertoshipped,
        to_date(TO_TIMESTAMP(at_shipped_date/1000000)) - dateadd(day,2,to_date(TO_TIMESTAMP(at_shipped_date/1000000))) as shippedtorequireddelta,
        shippedtorequireddelta::int > 0 as shippedontime
    from {{source('fudgeflix_V3','ff_account_titles')}}
)
select  
    o.orderid, 
    o.customerkey,
    o.orderdatekey,
    o.shippeddatekey,
    o.requireddatekey,
    o.ordertype,
    od.quantity,
    o.daysfromordertoshipped,
    o.shippedtorequireddelta,
    o.shippedontime
from stg_orders o
join stg_order_details od on o.orderid = od.orderid
UNION
select  
    a.*
from stg_account_titles a