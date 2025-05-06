/*
===============================================================================
Customer Report
===============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages, and transaction details.
	2. Segments customers into categories (VIP, Regular, New) and age groups.
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
===============================================================================
*/

if object_id('gold.report_customers','V') IS NOT NULL
	DROP VIEW gold.reort_Customers;
go

create view gold.report_customers as
with base_query as
(select
	sales.customer_key,
	cust.customer_number,
	concat(cust.first_name,' ', cust.last_name) as customer_name,
	datediff(year, cust.birthdate, GETDATE()) as age,
	sales.product_key,
	sales.order_number,
	sales.sales_amount,
	sales.quantity,
	sales.price,
	sales.order_date
		
from 
	gold.fact_sales sales
left join 
	gold.dim_customers cust
on sales.customer_key = cust.customer_key
where order_date is not null)



/*3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)*/
,customer_aggregation as 
(select
	customer_key,
	customer_number,
	customer_name,
	age,
	count(distinct order_number) as total_orders,
	sum(sales_amount) as total_sales,
	count(distinct product_key) as total_products,
	sum(quantity) as total_quantity,
	datediff(month, min(order_date), GETDATE()) as lifespan,
	max(order_date) as last_order_date
from 
	base_query
group by 
	customer_key,
	customer_number,
	customer_name,
	age)

select 
	customer_key,
	customer_number,
	customer_name,
	age,
	case
		 when age < 12 then 'Below 20'
		 when age between 20 and 29 then '20-29'
		 when age between 30 and 39 then '30-39'
		 when age between 40 and 49 then '40-49'
		 else 'Above 50'
	end as age_group,
	total_orders,
	total_sales,
	total_products,
	total_quantity,
	lifespan,
	case when lifespan >=12 and total_sales > 5000 then 'VIP'
		 when lifespan >=12 and total_sales <= 5000 then 'Regular'
		 else 'New Customer'
	end as customer_segment,
	--recency 
	datediff(month, last_order_date, getdate()) as recency,
	case 
		when total_sales = 0 then 0
		else total_sales/total_orders 
	end as avg_order_value,
	case 
		when lifespan = 0 then total_sales
		else total_sales/lifespan 
	end as avg_monthly_spend
	
from 
customer_aggregation