# sql-data-warehouse
Building a modern DWH with SQL server using ETL processes, data modeling and analytics

I am going to try and build a data warehouse from scratch.
I am going to use Notion for project planning, draw.io for drawing the data architecture which is the medallion architecture

The source data is coming from 2 systems - CRM and ERP

![image](https://github.com/user-attachments/assets/fc0cc881-09e6-4478-98e8-891ebb52e48b)





INITIALIZATION : File - 00_init_database
1. I have created a new database for this project
2. Since I have chosen medallion architecture - I created 3 schemas - Bronze, Silver and Gold.

   
BRONZE LAYER : File - 01_ddl_bronze, 02_proc_load_bronze
1. Here I have loaded the data as is from source.
2. I have created a new table using the same name as that of the source files.
3. The naming convention for the tables is <source_entity>_<name_of_file>
4. I have created a stored procedure for truncating all tables and loading the data into bronze tables.

SILVER LAYER : 03_ddl_silver, 04_proc_load_silver
1. Created new tables for silver schema
2. Performed data cleansing, standardized/normalized data and performed validations to check correctness of data
3. Stored procedure to truncate all tables and load all data into Silver tables

GOLD_LAYER: 05_ddl_gold.sql
1. Creates new views by integrating data from schema tables
   Steps :
      Select columns needed from master table
      Perform left join for other tables
      Look for duplicates to see if join has introduced no duplicates
      Data Integration - find which is the master source fr these tables for data integration
      Rename columns to friendly names
      Group relevant columns together
      Create surrogate key - just a row number using row_number()
      Follow naming convention for view gold.dim_customers
      Perform quality check
      If fact table group in the order -  keys - dates - measures

Data flow diagram : 
   ![image](https://github.com/user-attachments/assets/7a6b5bbe-810d-4c83-856e-1021f6d7c971)

