INSERT INTO silver.crm_cust_info (
	cst_id
	,cst_key
	,cst_firstname
	,cst_lastname
	,cst_marital_status
	,cst_gndr
	,cst_create_date
	)
SELECT cst_id
	,cst_key
	,trim(cst_firstname) AS cst_firstname
	,trim(cst_lastname) AS cst_lastname
	,CASE UPPER(trim(cst_marital_status))
		WHEN 'M'
			THEN 'Male'
		WHEN 'F'
			THEN 'Female'
		ELSE 'Unknown'
		END AS cst_marital_status
	,CASE UPPER(trim(cst_gndr))
		WHEN 'M'
			THEN 'Married'
		WHEN 'S'
			THEN 'Single'
		ELSE 'Unknown'
		END AS cst_gndr
	,cst_create_date
FROM (
	SELECT *
		,row_number() OVER (
			PARTITION BY cst_id ORDER BY cst_create_date DESC
			) AS number
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	) A
WHERE number = 1;



INSERT INTO silver.crm_prd_info (
	prd_id
	,cat_id
	,prd_key
	,prd_nm
	,prd_cost
	,prd_line
	,prd_start_dt
	,prd_end_dt
	)
SELECT prd_id
	,replace(substring(prd_key, 1, 5), '-', '_') AS cat_id
	,substring(prd_key, 7, len(prd_key)) AS prd_key
	,prd_nm
	,isnull(prd_cost, 0) AS prd_cost
	,CASE trim(prd_line)
		WHEN 'M'
			THEN 'Mountain'
		WHEN 'R'
			THEN 'Road'
		WHEN 'S'
			THEN 'Other Sales'
		WHEN 'T'
			THEN 'Touring'
		ELSE 'Unknown'
		END AS prd_line
	,prd_start_dt
	,dateadd(day, - 1, lead(prd_start_dt) OVER (
			PARTITION BY prd_key ORDER BY prd_start_dt
			)) AS prd_end_dt
FROM bronze.crm_prd_info


