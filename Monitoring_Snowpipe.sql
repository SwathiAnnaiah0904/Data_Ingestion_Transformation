--Monitoring Pipe status--
Use role accountadmin;
Use WAREHOUSE COMPUTE_WH;
Use Database CR_PL;
Use schema PL_CR_STREAM;

select * from SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY where pipe_name = 'Cr_auto_stream';
select * from SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY;
select * from SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY where catalog_name = 'CR_PL';
select * from SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY where service_type = 'PIPE';
  