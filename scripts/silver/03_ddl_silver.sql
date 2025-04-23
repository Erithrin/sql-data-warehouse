/*

==================================
DDL Script : Create Silver Tables
==================================

Scripts : These DDL scripts are used to create tables in the SILVER schema using the same name
as source table from bronze schema. Additional metadata is added to help track the time of record creation.

Warning : If table exists, the table will be dropped and will be newly created

*/



if object_id('silver.crm_cust_info','U') is not null 
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

if object_id('silver.crm_prd_info','U') is not null
drop table silver.crm_prd_info;
go

create table silver.crm_prd_info (
prd_id int,
cat_id nvarchar(50),
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date,
dwh_create_date datetime2 default getdate()
);
go

if object_id('silver.crm_sales_details','U') is not null 
drop table silver.crm_sales_details;
go

create table silver.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt date,
sls_ship_dt date,
sls_due_dt date,
sls_sales int,
sls_quantity int,
sls_price int,
dwh_create_date datetime2 default getdate()
)
go

if object_id('silver.erp_cust_az12','U') is not null 
drop table silver.erp_cust_az12;
go

create table silver.erp_cust_az12(
CID nvarchar(50),
BDATE date,
GEN nvarchar(50),
dwh_create_date datetime2 default getdate()
);
go

if object_id('silver.erp_loc_a101','U') is not null
drop table silver.erp_loc_a101;
go

create table silver.erp_loc_a101(
CID nvarchar(50),
CNTRY nvarchar(50),
dwh_create_date datetime2 default getdate()
);
go

if object_id('silver.erp_px_cat_g1v2','U') is not null
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