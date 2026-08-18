-- RAW.CONFORMED.DATEDIMENSIONRAW.CONFORMED.DATEDIMENSION
-- create databases
create database if not exists analytics;
create database if not exists raw;
--create schemas
create schema if not exists analytics.northwind;
create schema if not exists analytics.fudgecompanies;
create schema if not exists raw.northwind;
create schema if not exists raw.conformed;
create schema if not exists raw.fudgemart_v3;
create schema if not exists raw.fudgeflix_v3;
-- define file formats
create or replace file format RAW.PUBLIC.PARQUET
TYPE = parquetRAW.NORTHWIND.CUSTOMERSRAW.NORTHWIND.ORDER_DETAILS
REPLACE_INVALID_CHARACTERS = TRUE;
create or replace file format RAW.PUBLIC.JSONARRAY
TYPE = json
STRIP_OUTER_ARRAY = TRUE;
create or replace file format RAW.PUBLIC.JSON
TYPE = json
STRIP_OUTER_ARRAY = FALSE;
create or replace file format RAW.PUBLIC.CSVHEADER
TYPE = 'csv'
FIELD_DELIMITER = ','
SKIP_HEADER=1;
create or replace file format RAW.PUBLIC.CSV
TYPE = csv
FIELD_DELIMITER = ','
PARSE_HEADER = FALSE
SKIP_HEADER = 0;
-- create stages
-- varying file formats
CREATE or replace STAGE RAW.PUBLIC.externalworld_files
URL = 'azure://externalworld.blob.core.windows.net/files/';
-- these are all parquet file formats
CREATE or replace STAGE RAW.PUBLIC.externalworld_database
URL = 'azure://externalworld.blob.core.windows.net/database/'
FILE_FORMAT = RAW.PUBLIC.PARQUET ;
-- stage the date dimension
CREATE or REPLACE TABLE raw.conformed.datedimension (
datekey int
,date date
,datetime timestamp
,year int
,quarter int
,quartername varchar(2)
,month int
,monthname varchar(3)
,day int
,dayofweek int
,dayname varchar(3)
,weekday varchar(1)
,weekofyear int
,dayofyear int
) AS
WITH CTE_MY_DATE AS (
SELECT DATEADD(DAY, SEQ4(), '1970-01-01 00:00:00') AS MY_DATE
FROM TABLE(GENERATOR(ROWCOUNT=>365*30))
)
SELECT
REPLACE(TO_DATE(MY_DATE)::varchar,'-','')::int as datekey,
TO_DATE(MY_DATE) as date
,TO_TIMESTAMP(MY_DATE) as datetime
,YEAR(MY_DATE) as year
,QUARTER(MY_DATE) as quarter
,CONCAT('Q', QUARTER(MY_DATE)::varchar) as quartername
,MONTH(MY_DATE) as month
,MONTHNAME(MY_DATE) as monthname
,DAY(MY_DATE) as day
,DAYOFWEEK(MY_DATE) as dayofweek
,DAYNAME(MY_DATE) as dayname
,case when DAYOFWEEK(MY_DATE) between 1 and 5 then 'Y' else 'N' end as
weekday
,WEEKOFYEAR(MY_DATE) as weekofyear
,DAYOFYEAR(MY_DATE) as dayofyear
FROM CTE_MY_DATE
;

--Create the Fudgemart Directory
-- stage Fudgemart customers
create or replace table RAW.Fudgemart_V3.fm_customers
(
customer_id int,
customer_email varchar,
customer_firstname varchar,
customer_lastname varchar,
customer_address varchar,
customer_city varchar,
customer_state char(2),
customer_zip varchar,
customer_phone varchar,
customer_fax varchar

);
copy into RAW.Fudgemart_v3.fm_customers
FROM '@RAW.PUBLIC.externalworld_database/fudgemart_v3.fm_customers.parquet'
MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE';

--stage Fudgemart orders
create or replace table RAW.Fudgemart_V3.fm_orders
(
order_id int,
customer_id int,
order_date int,
shipped_date int,
ship_via varchar,
creditcard_id int
);
copy into RAW.Fudgemart_V3.fm_orders
from '@RAW.PUBLIC.externalworlD_database/fudgemart_v3.fm_orders.parquet'
MATCH_BY_COLUMN_NAME='CASE_INSENSITIVE';

--stage Fudgeflix accounts
create or replace table RAW.FUDGEFLIX_V3.ff_accounts
(
account_id int,
account_email varchar,
account_firstname varchar,
account_lastname varchar,
account_address varchar,
account_zipcode char(5),
account_plan_id int,
account_opened_on datetime
);
copy into RAW.FUDGEFLIX_V3.ff_accounts
from '@RAW.PUBLIC.externalworlD_database/fudgeflix_v3.ff_accounts.parquet'
MATCH_BY_COLUMN_NAME= 'CASE_INSENSITIVE';

--STAGE Fudgeflix zipcodes
create or replace table raw.fudgeflix_v3.ff_zipcodes
(
zip_code char(5),
zip_city varchar,
zip_state char(2)
);
copy into RAW.FUDGEFLIX_V3.ff_zipcodes
from'@RAW.PUBLIC.externalworlD_database/fudgeflix_v3.ff_zipcodes.parquet'
MATCH_BY_COLUMN_NAME= 'CASE_INSENSITIVE';

--stage Fudgeflix account_titles
create or replace table raw.fudgeflix_v3.ff_account_titles
(
at_id int,
at_account_id int,
at_title_id varchar,
at_queue_date int,
at_shipped_date int,
at_returned_date int,
at_rating int
);
copy into raw.fudgeflix_v3.ff_account_titles
from '@RAW.PUBLIC.externalworlD_database/fudgeflix_v3.ff_account_titles.parquet'
MATCH_BY_COLUMN_NAME= 'CASE_INSENSITIVE';

--stage Fudgeflix titles
create or replace table raw.fudgeflix_v3.ff_titles
(
title_id varchar(20),
title_name varchar,
title_type varchar,
title_synopsis varchar,
title_avg_rating decimal(18,2),
title_release_year int,
title_runtime int,
title_rating varchar,
title_bluray_available boolean,
title_dvd_available boolean,
title_instant_available boolean,
title_date_modified int
);
copy into raw.fudgeflix_v3.ff_titles
from '@RAW.PUBLIC.externalworlD_database/fudgeflix_v3.ff_titles.parquet'
MATCH_BY_COLUMN_NAME= 'CASE_INSENSITIVE';

create or replace table raw.fudgemart_v3.fm_products
(
poduct_id int,
product_department varchar,
product_name varchar,
product_retail_price decimal(18,2),
product_wholesale_price decimal(18,2),
product_is_active boolean,
product_add_date int,
product_vendor_id int,
product_description varchar
);
copy into raw.fudgemart_v3.fm_products
from '@RAW.PUBLIC.externalworlD_database/fudgemart_v3.fm_products.parquet'
MATCH_BY_COLUMN_NAME= 'CASE_INSENSITIVE';