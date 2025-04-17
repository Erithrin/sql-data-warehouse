# sql-data-warehouse
Building a modern DWH with SQL server using ETL processes, data modeling and analytics

I am going to try and build a data warehouse from scratch.
I am going to use Notion for project planning, draw.io for drawing the data architecture which is the medallion architecture

The source data is coming from 2 systems - CRM and ERP

INITIALIZATION : File - 00_init_database
1. I have created a new database for this project
2. Since I have chosen medallion architecture - I created 3 schemas - Bronze, Silver and Gold.

   
BRONZE LAYER : File - 01_ddl_bronze, 02_proc_load_bronze
1. Here I have loaded the data as is from source.
2. I have created a new table using the same name as that of the source files.
3. The naming convention for the tables is <source_entity>_<name_of_file>
4. I have created a stored procedure for truncating all tables and loading the data into bronze tables.

