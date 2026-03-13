with stge_orders as 
(
    select        OrderID,  
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey, 
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey, 
        replace(to_date(orderdate)::varchar,'-','')::int as orderdate
        from {{source("northwind","Orders")}}
        )
,
stge_details as(
    select         OrderID as OI,
        {{dbt_utils.generate_surrogate_key(['productid'])}},
        quantity,
        (quantity * unitprice) as extendedpriceamount,
        (extendedpriceamount * discount) as discountamount,
        (extendedpriceamount-discountamount)

        from {{source("northwind","Order_Details")}}
)
select * 
from stge_orders o
    join stge_details d on o.OrderID = d.OI

