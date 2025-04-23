/*

==================================
DDL Script : Create Silver Tables
==================================

Scripts : These DDL scripts are used to create tables in the SILVER schema using the same name
as source table from bronze schema. Additional metadata is added to help track the time of record creation.

Warning : If table exists, the table will be dropped and will be newly created

*/



if exists(select 1 from information_schema.tables where table_name = 'silver.crm_cust_info')
drop table silver.crm_cust_info;
go

create table silver.crm_cust_info
(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date ,
dwh_create_date datetime2 default getdate()
);

go

if exists(select 1 from INFORMATION_SCHEMA.tables where table_name = 'silver.crm_prd_info')
drop table silver.crm_prd_info;
go

create table silver.crm_prd_info (
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 default getdate()
);
go


if exists(select 1 from INFORMATION_SCHEMA.tables where table_name = 'silver.crm_sales_details')
drop table silver.crm_sales_details;
go

create table silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 default getdate()
)
go

if exists(select 1 from INFORMATION_SCHEMA.tables where table_name = 'silver.erp_cust_az12')
drop table silver.erp_cust_az12;
go

create table silver.erp_cust_az12(
CID nvarchar(50),
BDATE date,
GEN nvarchar(50),
dwh_create_date datetime2 default getdate()
);
go


if exists(select 1 from INFORMATION_SCHEMA.tables where table_name = 'silver.erp_loc_a101')
drop table silver.erp_loc_a101;
go

create table silver.erp_loc_a101(
CID nvarchar(50),
CNTRY nvarchar(50),
dwh_create_date datetime2 default getdate()
);
go


if exists(select 1 from INFORMATION_SCHEMA.tables where table_name = 'silver.erp_px_cat_g1v2')
drop table silver.erp_px_cat_g1v2;
go

create table silver.erp_px_cat_g1v2(
ID nvarchar(50),
CAT nvarchar(50),
SUBCAT nvarchar(50),
MAINTENANCE nvarchar(50),
dwh_create_date datetime2 default getdate()
);
go