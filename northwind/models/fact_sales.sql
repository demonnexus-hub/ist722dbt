with stge_orders as 
(
    select        OrderID,  
        employeeid as employeekey, 
       customerid as customerkey, 
        replace(to_date(orderdate)::varchar,'-','')::int as orderdate
        from {{source("northwind","Orders")}}
        )
,
stge_details as(
    select         OrderID as OI, productid as productkey,
        quantity,
        (quantity * unitprice) as extendedpriceamount,
        (extendedpriceamount * discount) as discountamount,
        (extendedpriceamount-discountamount) as soldamount

        from {{source("northwind","Order_Details")}}
)
select * 
from stge_orders o
    join stge_details d on o.OrderID = d.OI

