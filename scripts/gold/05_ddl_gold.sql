/*

==================================
DDL Script : Create Gold Views 
==================================

Scripts : These DDL scripts are used to create views in the GOLD schema. Represents final dimension and fact tables.

Gold layer is formed by combining useful data from the silver tables and is used for business analytics and reporting 
Usage : These views can be directly queried for analytics and reporting

Warning : If the view exists, the view will be dropped and will be recreated

*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
create view gold.dim_customers as

select 
	row_number() over(order by ci.cst_create_date) as customer_index,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as customer_firstname,
	ci.cst_lastname as customer_lastname,
	er.cntry as country,
	case 
		when ci.cst_gndr != 'Unknown' then ci.cst_gndr
		else coalesce(ep.gen,'Unknown') 
	end as gender,
	ci.cst_marital_status as marital_status,	
	ep.bdate as birthdate,
	ci.cst_create_date as created_date
	
	
from 
	silver.crm_cust_info ci
left join 
	silver.erp_cust_az12 ep
on 
	ci.cst_key = ep.cid 
left join 
	silver.erp_loc_a101 er
on ci.cst_key = er.cid;
go 

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================


if object_id('gold.dim_products','V') is not null
	drop view gold.dim_products;
go


CREATE VIEW gold.dim_products as
SELECT row_number() OVER (
		ORDER BY crm.prd_start_dt,
			crm.prd_key
		) AS product_key, -- surrogate key
	crm.prd_id AS product_id,
	crm.prd_key AS product_number,
	crm.prd_nm AS product_name,
	crm.cat_id AS category_id,
	erp.cat AS category,
	erp.subcat AS subcategory,
	erp.maintenance,
	crm.prd_cost AS product_cost,
	crm.prd_line AS product_line,
	crm.prd_start_dt AS start_date
FROM silver.crm_prd_info crm
LEFT JOIN silver.erp_px_cat_g1v2 erp ON crm.cat_id = erp.id
WHERE crm.prd_end_dt IS NULL; -- filtering out historical data

go

-- =============================================================================
-- Create Dimension: gold.fact_sales
-- =============================================================================

if object_id('gold.fact_sales', 'V') is not null
	drop view gold.fact_sales;
go


create view gold.fact_sales as 
SELECT  
	sls_ord_num AS order_number,
	prod.product_key,
	cust.customer_index AS customer_key,
	sls_order_dt AS order_date,
	sls_ship_dt AS shipping_date,
	sls_due_dt AS due_date,
	sls_sales AS sales_amount,
	sls_quantity AS quantity,
	sls_price AS price
FROM silver.crm_sales_details sales
LEFT JOIN 
	gold.dim_customers cust 
ON 
	sales.sls_cust_id = cust.customer_id
LEFT JOIN 
	gold.dim_products prod 
ON 
	sales.sls_prd_key = prod.product_number;

go