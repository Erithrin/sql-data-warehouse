/*
==================================================
Part to whole Analysis
==================================================
Purpose: 
-To track contribution of individual part to overall performance
-Understand which category has the greatest impact on business

SQL Functions used :
- sum() over()
*/


-- Which categories contribute the most to overall sales?
with category_sales as (select 
	category,
	sum(sales_amount) as sales_by_category
from 
	gold.fact_sales sale
left join 
	gold.dim_products prod 
on sale.product_key = prod.product_key
group by 
	category)

select 
	category,
	sales_by_category,
	sum(sales_by_category) over() as full_sales,
	concat(round((cast(sales_by_category as float)/sum(sales_by_category) over()) * 100,2),'%') as contribution
from 
	category_sales
order by contribution desc