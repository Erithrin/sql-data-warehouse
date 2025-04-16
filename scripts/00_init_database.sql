/*
create database and schemas

Script purpose : This script creates a new database called DataWarehouse if it does not exist.
It the database exists, it is dropped and recreated.
We also setup three schemas - Bronze, Silver ad Gold

WARNING: This script deletes the existing database - DataWarehouse if it exists
All data will be permanently deleted so proceed with caution. Take backup if needed for restoration purpose

*/


use master;
go

--create database DataWarehouse
create database DataWarehouse;
go

if exists(select 1 from sys.databases where name = 'DataWarehouse')
begin
	alter database DataWarehouse set single_user with rollback immediate;
	drop database DataWarehouse;
end;
use DataWarehouse;
go

--create schema gold - ingestion layer
create schema bronze;
go

--create schema silver - transformation layer
create schema silver;
go

--create schema gold - business layer
create schema gold;
go

