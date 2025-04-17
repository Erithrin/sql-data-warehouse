/*
Scripts : These DDL scripts are used to create tables in the BRONZE schema using the same name
as source table. 
Warning : If table exists, the table will be dropped and will be newly created
*/


if exists(select 1 from information_schema.tables where table_name = 'bronze.crm_cust_info')
drop table bronze.crm_cust_info;
go

/*
if object_id('bronze.crm_cust_info') is not null
drop table bronze.crm_cust_info;
go
*/

/*using  nvarchar here so that we can store special characters; if any*/

create table bronze.crm_cust_info
(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date 
);

go


if exists(select 1 from INFORMATION_SCHEMA.tables where table_name = 'bronze.crm_prd_info')
drop table bronze.crm_prd_info;
go

create table bronze.crm_prd_info (
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_dt date,
prd_end_dt date
);
go


if exists(select 1 from INFORMATION_SCHEMA.tables where table_name = 'bronze.crm_sales_details')
drop table bronze.crm_sales_details;
go

create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
sls_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
)
go

if exists(select 1 from INFORMATION_SCHEMA.tables where table_name = 'bronze.erp_cust_az12')
drop table bronze.erp_cust_az12;
go

create table bronze.erp_cust_az12(
CID nvarchar(50),
BDATE date,
GEN nvarchar(50)
);
go


if exists(select 1 from INFORMATION_SCHEMA.tables where table_name = 'bronze.erp_loc_a101')
drop table bronze.erp_loc_a101;
go

create table bronze.erp_loc_a101(
CID nvarchar(50),
CNTRY nvarchar(50)
);
go


if exists(select 1 from INFORMATION_SCHEMA.tables where table_name = 'bronze.erp_px_cat_g1v2')
drop table bronze.erp_px_cat_g1v2;
go

create table bronze.erp_px_cat_g1v2(
ID nvarchar(50),
CAT nvarchar(50),
SUBCAT nvarchar(50),
MAINTENANCE nvarchar(50)
);
go