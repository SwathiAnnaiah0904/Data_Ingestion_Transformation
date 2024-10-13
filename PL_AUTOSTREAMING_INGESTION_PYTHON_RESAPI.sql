-----Automating/continuous data ingestion by data streaming and Snowpipe using Python RestAPI------

Use role accountadmin;
use warehouse compute_wh;
use database CR_PL;


--Creating Schema 
create or replace schema  PL_CR_Stream;

--Creating table
create or replace transient table customer_raw_auto_ingest (
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
select * from customer_raw_auto_ingest;


--Creating file format
create or replace file format csv_cr_auto
type = csv
compression = 'none'
field_delimiter=','
record_delimiter='\n'
skip_header = 1
field_optionally_enclosed_by = '\047';


--Creating Internal stage 
create or replace stage stg_auto
file_format = csv_cr_auto
show stages;
desc stage stg_auto;

--Copying file to stage
put 'file://C:\\Users\\swath\\OneDrive\\PL\\input_csv_date_format.csv'; @stg_auto Auto_Compress=false; //loaded data using put command on Snowsql
list @stg_auto;

--Copying data to table  
copy into customer_raw_auto_ingest from @stg_auto;
select * from customer_raw_auto_ingest;


--Creating Snowpipe 
Create or replace pipe Cr_auto_stream
as copy into customer_raw_auto_ingest from @stg_auto/delta/;
desc pipe cr_auto_stream;

---Run pipe
alter pipe Cr_auto_stream refresh;

--copying data file to delta
put 'file://C:\\Users\\swath\\OneDrive\\PL\\input_csv_date_format.csv'; @stg_auto/delta/ Auto_Compress=false; //loaded data using put command on Snowsql


--Python connector with RSA private and public key - for RESTApi Connection
Alter User SWATHIANNAIAH09 set RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAuxzucew3UguTqUkKGwSb
XOv0w1J9uTLTsOKkBqZ0YnVeyJ16BEsyAV1l9C0Qeih8lGv4VZdY4/hTy7a5cSDk
gZ0y4yGQ3zxC6540n7ceTmq3dviyl0Tsa8LUt8atZcbYhl2b2eOZ6hQK7bL+TGHA
rcHhsrnTKTIerTXnmoFbABQ5Ou44cFskfXEZAGaOb5o/Ty0pQmebiINFNGehfUDS
A6y5CI5rB6DtNe0GjWSbU7TcNwzDcBWvBgxTIuJuvFjyHvPj0rbtc2MwlrBDfqXI
UhylURvnt1QFI4czWE+P40qomMuk/IxSaYzLjF2BiMkgl/H0M8r5/DSm7S+tAZje
KQIDAQAB'

DESC USER SWATHIANNAIAH09;

--Listing files in delta and table
list @stg_auto/delta/;
select * from customer_raw_auto_ingest;

---Pause the pipe
alter pipe Cr_auto_stream set pipe_execution_paused = true;
---Run pipe
alter pipe Cr_auto_stream refresh;

----Validating pipe status
select system$pipe_status('Cr_auto_stream');
select * from table(validate_pipe_load(
pipe_name=> 'Cr_auto_stream',
start_time=>dateadd(hour,-1,current_timestamp())));

select SYSTEM$PIPE_STATUS('Cr_auto_stream');