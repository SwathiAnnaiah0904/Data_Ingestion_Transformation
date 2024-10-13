# Data_Ingestion_Transformation
Data Ingestion and transformation process using Snowflake, AWS s3, Python Rest API connectors

Objective : Ingest data from external data resource, clean, transfrom, automate following best practices using the technologies in the instructions. 

Considering input file: ```/input_csv_date_format```.

## Task and Solution

# Task 1
Ingesting and Transforming given input data into required output

# Solution
-Ingested input csv file from AWS S3 into Snowflake WebUI , using sql commands; 
- Used 'COPY_INTO' SQL Command to ingest the file on Snowflake External Stage
  -SQL_file listed here: ```/PL_AWS_INGESTION_TRANSFORMATION```
- Transfomed ingested input data(image 1.1) into desired output(image 1.2) and stored on snowflake external stage table.
   - Desired Output
      -  a) Enriched data by transforming CustomerSegment with "Active customer" or "Inactive customer" based on PurchaseDate
      -  b) Aggregated Total amount spent by each customer (TotalSpent).

  **Insight** : Manual Ingestion of data using Copy_into command, High run time compared other options, no automated flow, facilitates manual batch data load. 
  

# Task 2
Transforming and Automating give input data into required output. 

# Solution
  - Transformed the input data and stored the output results on External stage table in previous Task 1.
  - Used 'TASK' command frow Snowsql to scheduled automate filtering of output data  to get desired insights.
    - SQL_file listed here:  ```PL_AUTOMATE_INGESTION_TRANSFORMATION_TASKS```
    - Desired Output
      - Filtered customers who opted in for promotions (PromoOptIn = TRUE) (image 2.2).
  -  Created and **Automated** series of tasks composed of a root task and child tasks, organized by their dependencies and scheduled run (image 2.4).
      -  Root task to retrieve data from Task 1 Output table and create validated table auto scheduled every one minute (image 2.1)
      -  Child task 1 to filter customers who opted in for promotions (PromoOptIn = TRUE) (image 2.2)
      -  Child task 2 to filter cutsomers who are active(CustomerSegment = 'Active customers') (image 2.3)
      -  Created graphical representation of DAG for the above to visualise tasks dependencies (image 2.4)

 **Insight** : Automated transformation or even can create data ingestion using Tasks (DAGs) for scheduled flow, High run time compared to Snowpipe and Snowflake streaming, faciliates batch data load.
 

# Task 3
Automating Data Ingestion from AWS S3 to Snowflake External Stage for continuous data loading Using AWS Event notification trigger. 

# Solution
- Created External storage integration between AWS S3 and Snowflake configuring AWS event Notification trigger (SQS)
- Created External stage, followed by data table to load data from AWS
- Created and refreshing the Snowpipe/pipe to integrate with AWS S3 to auto ingest the input data when event notification is triggered on AWS
  - SQL_file listed here:  ```PL_AUTOMATE_INGESTION_SNOWPIPE```
- Verification and monitoring pipe status for managing the external integration data flow.

 **Insight** : Automated Ingestion for triggered/continuous flow, Less run time compared to Tasks, medium run time overall, easy to verify and monitor pipe status and fail runs, facilitates batch and continous data load.


# Task 4
Automating Data Ingestion from Intergal Stage to Snowflake for continuous data loading using Python Rest API.

# Solution
  -   Created Internal stage, followed by data table to load data from local drive using "PUT" command on SnowSQL\SnowCLI. 
  -   Created and refreshing the Snowpipe/pipe to integrate with Python Rest API.
      -   SQL_file listed here: ```PL_AUTOSTREAMING_INGESTION_PYTHON_RESAPI```
  -   Configuring Python connector for snowflake with RSA private and public key - for RESTApi Connection
      -   Python_file listed here: ```PL_SNOWPIPE_STREAMING_PYTHON_CODE```
  -   Running Python code for connection on VS and loading facilitating continous data load. 
  -   Validating and monitoring pipe status for managing the external integration data flow.
      -   Montinoring_SQL here: ```Monitoring_Snowpipe```

 **Insight** : Automated Ingestion for continuos flow, least run time compared to all the above, facilitates real time and continuous data load, 
**Storage Insight** : Using Internal stage is cost effective especially when same set of tables repeatedly used for transformation.




 

    
  
  

