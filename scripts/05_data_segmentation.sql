/*
==================================================
Data Segmentation Analysis
==================================================
Purpose: 
- Grouping data into meaningful categories for targeted insights
- For customer segmentation, product categorization or regional analysis

SQL Functions used :
- case when 
- group by
*/


/*Segment products into cost ranges and count how many products fall into each segment*/


with product_segment as 
(select 
	product_name,
	cost,
	case when cost < 100 then 'Below 100'
		 when cost between 100 and 500 then '100-500'
		 when cost between 500 and 1000 then '500-1000'
		 else 'Above 1000'
	end as cost_range 
from 
	gold.dim_products)

select 
	cost_range,
	count(product_name) as total_products
from 
	product_segment
group by cost_range
order by total_products desc



/*Group customers into three segments based on their spending behavior:
	- VIP: Customers with at least 12 months of history and spending more than 5,000.
	- Regular: Customers with at least 12 months of history but spending 5,000 or less.
	- New: Customers with a lifespan less than 12 months.
And find the total number of customers by each group
*/

with customer_spending as
(select	
	customer_key,
	sum(sales_amount) as total_spend,
	datediff(month, min(order_date), max(order_date)) as lifespan
from 
	gold.fact_sales
group by customer_key)


select 
	customer_segment,
	count(customer_key) as total_customers
from 
	(select 
		customer_key,
		case when lifespan >=12 and total_spend > 5000 then 'VIP'
			 when lifespan >=12 and total_spend <= 5000 then 'Regular'
			 else 'New Customer'
		end as customer_segment
	from 
		customer_spending) t
group by customer_segment
order by total_customers desc