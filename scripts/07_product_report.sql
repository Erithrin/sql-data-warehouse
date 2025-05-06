/*
===============================================================================
Product Report
===============================================================================
Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
       - total orders
       - total sales
       - total quantity sold
       - total customers (unique)
       - lifespan (in months)
    4. Calculates valuable KPIs:
       - recency (months since last sale)
       - average order revenue (AOR)
       - average monthly revenue
===============================================================================
*/
if object_id('gold.report_products', 'V') is not null
drop view gold.report_products
go

create view gold.report_products as 
with base_query as
(select
	prod.product_key,
	prod.product_number,
	prod.product_name,
	prod.category,
	prod.subcategory,
	prod.cost,
	sales.order_number,
	sales.sales_amount,
	sales.quantity,
	sales.order_date,
	sales.customer_key
from 
	gold.fact_sales sales
left join
	gold.dim_products prod
on 
	prod.product_key = sales.product_key
)

,product_aggregation as
(select 
	product_key,
	product_number,
	product_name,
	category,
	subcategory,
	cost,
	count(distinct order_number) as total_orders,
	sum(sales_amount) as total_sales,
	count(quantity) as total_quantity,
	count(distinct customer_key) as total_unique_customers,
	datediff(month, min(order_date), GETDATE()) as lifespan,
	max(order_date) as last_order_date,
	round(avg(cast(sales_amount as float)/nullif(quantity,0)),2) as avg_selling_price
from 
	base_query
group by 
	product_key,
	product_number,
	product_name,
	category,
	subcategory,
	cost)

select 
	product_key,
	product_number,
	product_name,
	category,
	subcategory,
	cost,
	total_orders,
	total_sales,
	total_quantity,
	total_unique_customers,
	lifespan,
	avg_selling_price,
	case 
		when total_sales >50000 then 'High Performers'
		when total_sales >=10000 then 'Mid Performers' 
		else 'Low Performer'
	end as product_segment,
	--recency
	datediff(month, last_order_date, GETDATE()) as recency,
	--average order revenue (AOR)
	case 
		when total_orders = 0 then 0
		else total_sales/total_orders
	end as average_order_revenue,
	--- average monthly revenue
	case 
		when lifespan = 0 then 0
		else total_sales/lifespan
	end as average_monthly_revenue
from 
	product_aggregation