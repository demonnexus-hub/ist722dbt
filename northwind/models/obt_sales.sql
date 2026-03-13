with f_sales as (
    select * from {{ ref('fact_sales') }}
),
d_customer as (
    select * from {{ ref('dim_customer') }}
),
d_employee as (
    select * from {{ ref('dim_employee') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
),
d_product as (
    select * from {{ref('dim_product')}}
)

select F.orderid, c.customerid, e.employeeid, d.datekey,p.productid, f.quantity, f.extendedpriceamount, f.discountamount,f.soldamount 

from f_sales f 
    left join d_customer c on f.customerkey = c.customerid
    left join d_employee e on f.employeekey = e.employeeid
    left join d_date d on f.orderdate = d.datekey
    left join d_product p on f.productkey = p.productid