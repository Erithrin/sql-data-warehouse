/*
==================================================
Cumulative Analysis
==================================================
Purpose: 
-To track trends growth and changes in key metrics over time
-For time series analysis and identifying seasonality
-Measure growth or decline over specific periods

SQL Functions used :
- Datetrunc(), year(), month(), format()
-sum(), count()


*/


--tracking sales and price data over month
--calculate sales total and average price per month
--and running totals and moving avg over time

select 
	order_date,
	total_sales_amount,
	sum(total_sales_amount) over (order by order_date) as running_totals,
	avg(avg_price) over (order by order_date) as moving_average
from
	(select 
		datetrunc(month,order_date) as order_date,
		sum(sales_amount) as total_sales_amount,
		avg(price) as avg_price
	from 
		gold.fact_sales 
	where 
		order_date is not null 
	group by 
		datetrunc(month,order_date) 
	) t