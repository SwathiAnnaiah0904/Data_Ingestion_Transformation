---- Data Ingestion and Transformation from External stage S3 using COPY_INTO -----
Use role accountadmin;
Use Warehouse COMPUTE_WH;
CREATE
OR REPLACE DATABASE CR_PL;
USE DATABASE CR_PL;
----Loading data from AWS S3-----
--Creating external storage integration
CREATE
or replace STORAGE INTEGRATION AWS_STG TYPE = EXTERNAL_STAGE ENABLED = TRUE STORAGE_PROVIDER = S3 STORAGE_ALLOWED_LOCATIONS = ('s3://cr-data-raw-pl/cr_raw_pl/') STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::054037137143:role/PLROLE';
DESC STORAGE INTEGRATION AWS_STG;
--Creating external stage
CREATE
or replace STAGE AWS_ESTG URL = 's3://cr-data-raw-pl/cr_raw_pl/' STORAGE_INTEGRATION = AWS_STG;
LIST @AWS_ESTG;
--creating file format
create
or replace file format csv_cr type = csv compression = 'none' field_delimiter=',' record_delimiter='\n' skip_header = 1 field_optionally_enclosed_by = '\047';
--creating raw data table
create
or replace transient table customer_raw ( CustomerID String, Name String, Email String, PurchaseDate String, AmountSpent String, LastLogin string, Country string, CustomerSegment String, PromoOptIn string );
--loading data from external stage to table
copy into customer_raw from @AWS_ESTG file_format = csv_cr on_error = 'continue';
--List rows in the table
Select
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9
from
        @AWS_ESTG
Select
        *
from
        customer_raw;
--------------------------------------------------------------------------------------------------------------------
----------TRANSFORMING DATA------------
----Creating Table for validated data
create
or replace transient table customer_validated ( CustomerID INT, Name Varchar(60), Email Varchar(60), TotalSpent INT, LastLogin date, CustomerSegment Varchar(60), PromoOptIn boolean );
--To handle null values in datecolumn
Update
        customer_raw
set
        purchasedate =
        Case
        when
                purchasedate = 'NULL'
        then
                LastLogin
        else
                purchasedate
        end
----ACTIVE_USERS and TOTAL AMOUNT SPENT
Insert into customer_validated
        (
                CustomerID,
                Name      ,
                Email     ,
                TotalSpent,
                LastLogin ,
                PromoOptIn,
                CustomerSegment
        )
select
        Cast(CustomerID as INT)                    ,
        Name                                       ,
        Email                                      ,
        Sum(Cast(AmountSpent as INT))              ,
        To_Variant(To_date(LastLogin,'DD-MM-YYYY')),
        Cast(PromoOptIn as Boolean)                ,
        Case
        when
                datediff(day,To_date(purchasedate,'DD-MM-YYYY'),To_date(lastlogin,'DD-MM-YYYY'))    >180
                OR datediff(day,To_date(purchasedate,'DD-MM-YYYY'),To_date(lastlogin,'DD-MM-YYYY')) = 0
        then
                'Inactive Customers'
        else
                'Active Customers'
        end as CUSTOMERSEGMENT
from
        customer_raw
group by
        CUSTOMERID,
        NAME      ,
        EMAIL     ,
        LASTLOGIN ,
        PROMOOPTIN,
        purchasedate;
--List rows in the table
select
        *
from
        customer_validated;