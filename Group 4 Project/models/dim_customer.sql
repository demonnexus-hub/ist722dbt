with stg_customers as (
    select * from {{ source('fudgemart_V3','fm_customers')}}
),
stg_accounts as (
    select * from {{ source('fudgeflix_V3','ff_accounts')}} as a
    join {{ source('fudgeflix_V3','ff_zipcodes')}} as z on a.account_zipcode = z.zip_code
)
select  {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} as customer_key, 
    c.customer_id, 
    c.customer_email,
    c.customer_firstname,
    c.customer_lastname,
    c.customer_address,
    c.customer_city,
    c.customer_state,
    c.customer_zip,
    c.customer_phone,
    c.customer_fax,
    null as customer_plan_id,
    null as customer_opened_on
from stg_customers c
UNION
select  {{ dbt_utils.generate_surrogate_key(['a.account_id']) }} as customer_key, 
    a.account_id as customer_id,
    a.account_email as customer_email,
    a.account_firstname as customer_firstname,
    a.account_lastname as customer_lastname,
    a.account_address as customer_address,
    a.zip_city as customer_city,
    a.zip_state as customer_state,
    a.account_zipcode as customer_zip,
    null as customer_phone,
    null as customer_fax,
    a.account_plan_id as customer_plan_id,
    a.account_opened_on as customer_opened_on
from stg_accounts a
