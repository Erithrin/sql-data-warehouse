/*
==================================================
Performance Analysis
==================================================
Purpose: 
-To track trends growth and changes over year or month with a target value such as previous year
-For identifying high performing entities
-Measure performance of products, customers or regions over time

SQL Functions used :
- lag(),avg() over()
-sum(), count(),avg()


*/

/* Analyze the yearly performance of products by comparing their sales 
to both the average sales performance of the product and the previous year's sales */


with current_year_sales as 
(select 
	year(sales.order_date) as order_year,
	prod.product_name,
	sum(sales.sales_amount) as cy_sales

from 
	gold.fact_sales sales
left join 
	gold.dim_products prod
on 
	sales.product_key = prod.product_key
where sales.order_date is not null
group by
	year(sales.order_date),
	prod.product_name)


select 
	order_year,
	product_name,
	cy_sales,
	avg(cy_sales) OVER (PARTITION BY product_name) as avg_sales,
	cy_sales - avg(cy_sales) OVER (PARTITION BY product_name) as difference_avg,
	case 
		when cy_sales - avg(cy_sales) OVER (PARTITION BY product_name) < 0 then 'Below Avg'
		when cy_sales - avg(cy_sales) OVER (PARTITION BY product_name) > 0 then 'Above Avg'
		else 'Avg'
	end as avg_change,
	--yoy analysis 
	lag(cy_sales) over(partition by product_name order by order_year) as py_sales,
	cy_sales - lag(cy_sales) over(partition by product_name order by order_year) as difference_py,
	case 
		when cy_sales - lag(cy_sales) over(partition by product_name order by order_year) < 0 then 'Decrease'
		when cy_sales - lag(cy_sales) over(partition by product_name order by order_year) > 0 then 'Increase'
		else 'No Change'
	end as py_change
from 
	current_year_sales
order by product_name,order_year
