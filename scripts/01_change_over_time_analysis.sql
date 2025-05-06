/*
==================================================
Change Over Time Analysis
==================================================
Purpose: 
-To track trends growth and changes in key metrics over time
-For time series analysis and identifying seasonality
-Measure growth or decline over specific periods

SQL Functions used :
- Datetrunc(), year(), month(), format()
-sum(), count()


*/
-- sales performance over time
--quick date functions
select 
	year(order_date) as order_year,
	month(order_date) as order_month,
	sum(sales_amount) as total_sales_amount,
	sum(quantity) as total_quantity,
	count(distinct customer_key) as total_customers
from 
	gold.fact_sales
where
	order_date is not null
group by 
	year(order_date),month(order_date)
order by 
	year(order_date),month(order_date)

--using date trunc functions


select 
	datetrunc(month, order_date) as order_date,
	sum(sales_amount) as total_sales_amount,
	sum(quantity) as total_quantity,
	count(distinct customer_key) as total_customers
from 
	gold.fact_sales
where
	order_date is not null
group by 
	datetrunc(month, order_date)
order by 
	datetrunc(month, order_date)


--using format function

select 
	format(order_date,'yyyy-MM') as order_date,
	sum(sales_amount) as total_sales_amount,
	sum(quantity) as total_quantity,
	count(distinct customer_key) as total_customers
from 
	gold.fact_sales
where
	order_date is not null
group by 
	format(order_date,'yyyy-MM')
order by 
	format(order_date,'yyyy-MM')