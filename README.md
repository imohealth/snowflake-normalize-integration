## SNOWFLAKE NORMALIZE CONNECTOR

This is a sample app that demonstrates how you can normalize patient problem conditions to industry standard icd10cm codes. 

# Requirements
Install dependent packages with the following command
pip install -r requirements.txt

This will install the following python packages to your compute cluster

snowflake-connector-python
snowflake-connector-python[pandas]
snowflake-sqlalchemy


# Highlevel flow of this connector
![High Level Flow](resources/imo-snowflake-connector.drawio.png)

# Create Snowflake Resources
  * create TESTSNOWFLAKE database with default settings
  * create TEST_SCHEMA schema with default settings
  * create PATIENT_TERMS table with default settings
  ```SQL 
  create table PATIENT_TERMS (
    patient_id varchar(255),
    visit_date varchar(255),
    condition varchar(255)
  )
  ```
  * upload patient_conditons.csv to the new table

  ![Add Data](resources/add_data.png)

# Using the connector
- Step 1. Extraction of data from Snowflake Datawarehouse
  - Add M2M credentials in the config.json for Normalize API (ClientID and SecretKey)
  - Add Snowflake connection string details (USER, PWD, ACCOUNT, DATABASE AND SCHEMA)
  - Specify the write method for the script
    * Use direct to write normalization data directly to the Snowflake table
    * Use internal to write to an internal stage on the Snowflake table
  - Specify batch size (API max is 20 in a single request)
- Step 2  Send request to Normalize API
- Step 3  Get response from Normalize API
  Two options for Step 4
- Step 4a Write to internal SnowFlake Stage
- Step 4b. Write to external stage in AWS or Azure
- Step 5. Use Snowpipe to extract from internal or external stage and write to Snowflake staging table
- Step 6. Create a function or task to extract from staging table and write to Target table

This connector demonstrates how to write to internal Stage or write directly to Snowflake Target table


