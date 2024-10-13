---Automating data ingestion  using Snowpipe from S3 (External storage Integration)---

USE DATABASE CR_PL;

----Automate Loading data from AWS S3 through snowpipe-----

--Creating external storage integration
CREATE or replace STORAGE INTEGRATION AWS_STG_AUTO
TYPE = EXTERNAL_STAGE
ENABLED = TRUE
STORAGE_PROVIDER = S3
STORAGE_ALLOWED_LOCATIONS = ('s3://auto-cr-pl/auto-cr-raw/')
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::054037137143:role/PLAUTOROLE';

DESC STORAGE INTEGRATION AWS_STG_AUTO;


--Creating external stage

CREATE or replace STAGE AWS_ESTG_AUTO
URL = 's3://auto-cr-pl/auto-cr-raw/'
STORAGE_INTEGRATION = AWS_STG_AUTO;


--creating table 
create or replace transient table customer_raw_auto (
	CustomerID String,
	Name String,
	Email String,
    PurchaseDate String,
    AmountSpent String,
    LastLogin string,
    Country string,
    CustomerSegment String,
	PromoOptIn string
);


--Creating snowpipe to integrate with AWS S3
CREATE OR REPLACE PIPE CR_PIPE
AUTO_INGEST = TRUE
AS COPY INTO customer_raw_auto
from @AWS_ESTG_AUTO
file_format = (type = csv
field_delimiter = ','
skip_header = 1)



Show pipes;
list @AWS_ESTG_AUTO;


--verifying pipe status
select system$pipe_status('CR_PIPE');

--Loading Historic data
Alter pipe CR_PIPE refresh;

select * from customer_raw_auto;