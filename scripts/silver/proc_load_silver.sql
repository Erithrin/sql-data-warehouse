/*
===============================================================================
Stored Procedure: Load Silver Layer 
===============================================================================

SCRIPT PURPOSE :Peforms the Extract, Transform and Load process to  populate data from 
	bronze schema into silver schema after 
	1. Data Cleaning 
	2. Standardization & Normalization
	3. Validation 
PARAMETER :	 None

WARNING : This procedure truncates all previous data from tables and loads fresh data into silver schema

USAGE: exec silver.load_silver
https://poorsql.com/ 


===============================================================================
*/
CREATE OR

ALTER PROCEDURE silver.load_silver
AS
BEGIN
	DECLARE @BATCH_START DATETIME,
		@BATCH_END DATETIME,
		@START_TIME DATETIME,
		@END_TIME DATETIME;

	BEGIN TRY
		SET @BATCH_START = GETDATE();

		PRINT '==============================================';
		PRINT 'Loading Silver Layer';
		PRINT '==============================================';
		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';

		SET @START_TIME = GETDATE();

		PRINT 'Truncating table silver.crm_cust_info';

		TRUNCATE TABLE silver.crm_cust_info;

		PRINT 'Loading >> silver.crm_cust_info';

		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
			)
		SELECT cst_id,
			cst_key,
			trim(cst_firstname) AS cst_firstname,
			trim(cst_lastname) AS cst_lastname,
			CASE UPPER(trim(cst_marital_status))
				WHEN 'M'
					THEN 'Male'
				WHEN 'F'
					THEN 'Female'
				ELSE 'Unknown'
				END AS cst_marital_status,
			CASE UPPER(trim(cst_gndr))
				WHEN 'M'
					THEN 'Married'
				WHEN 'S'
					THEN 'Single'
				ELSE 'Unknown'
				END AS cst_gndr,
			cst_create_date
		FROM (
			SELECT *,
				row_number() OVER (
					PARTITION BY cst_id ORDER BY cst_create_date DESC
					) AS number
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			) A
		WHERE number = 1;

		PRINT 'Completed >> silver.crm_cust_info';

		SET @END_TIME = GETDATE();

		PRINT 'Load Duration = ' + CAST(datediff(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------';

		SET @START_TIME = GETDATE();

		PRINT 'Truncating table silver.crm_prd_info';

		TRUNCATE TABLE silver.crm_prd_info;

		PRINT 'Loading >> silver.crm_prd_info';

		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
			)
		SELECT prd_id,
			replace(substring(prd_key, 1, 5), '-', '_') AS cat_id,
			substring(prd_key, 7, len(prd_key)) AS prd_key,
			prd_nm,
			isnull(prd_cost, 0) AS prd_cost,
			CASE trim(prd_line)
				WHEN 'M'
					THEN 'Mountain'
				WHEN 'R'
					THEN 'Road'
				WHEN 'S'
					THEN 'Other Sales'
				WHEN 'T'
					THEN 'Touring'
				ELSE 'Unknown'
				END AS prd_line,
			prd_start_dt,
			dateadd(day, - 1, lead(prd_start_dt) OVER (
					PARTITION BY prd_key ORDER BY prd_start_dt
					)) AS prd_end_dt
		FROM bronze.crm_prd_info;

		PRINT 'Completed >> silver.crm_prd_info';

		SET @END_TIME = GETDATE();

		PRINT 'Load Duration = ' + CAST(datediff(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------';

		SET @START_TIME = GETDATE();

		PRINT 'Truncating table silver.crm_sales_details';

		TRUNCATE TABLE silver.crm_sales_details;

		PRINT 'Loading >> silver.crm_sales_details';

		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
			)
		SELECT sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt IS NULL OR sls_order_dt = 0 OR len(sls_order_dt) != 8
					THEN NULL
				ELSE cast(cast(sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt IS NULL OR sls_ship_dt = 0
					THEN NULL
				ELSE cast(cast(sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt IS NULL OR sls_due_dt = 0
					THEN NULL
				ELSE cast(cast(sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,
			CASE 
				WHEN sls_sales IS NULL OR sls_sales != sls_quantity * abs(sls_price)
					THEN sls_quantity * abs(sls_price)
				ELSE sls_sales
				END AS sls_sales,
			sls_quantity,
			CASE 
				WHEN sls_price IS NULL OR sls_price <= 0
					THEN sls_sales / isnull(sls_quantity, 0)
				ELSE sls_price
				END AS sls_price
		FROM bronze.crm_sales_details;

		PRINT 'Completed >> silver.crm_sales_details';

		SET @END_TIME = GETDATE();

		PRINT 'Load Duration = ' + CAST(datediff(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------';
		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';

		SET @START_TIME = GETDATE();

		PRINT 'Truncating table silver.erp_cust_az12';

		TRUNCATE TABLE silver.erp_cust_az12;

		PRINT 'Loading >> silver.erp_cust_az12';

		INSERT INTO silver.erp_cust_az12 (
			cid,
			bdate,
			gen
			)
		SELECT CASE 
				WHEN CID LIKE 'NAS%'
					THEN substring(CID, 4, len(CID))
				ELSE CID
				END AS CID,
			CASE 
				WHEN BDATE > getdate()
					THEN NULL
				ELSE bdate
				END AS bdate,
			CASE 
				WHEN upper(trim(gen)) = 'M'
					THEN 'Male'
				WHEN upper(trim(gen)) = 'F'
					THEN 'Female'
				WHEN trim(gen) = '' OR gen IS NULL
					THEN 'Unknown'
				ELSE gen
				END AS gen
		FROM bronze.erp_cust_az12;

		PRINT 'Completed >> silver.erp_cust_az12';

		SET @END_TIME = GETDATE();

		PRINT 'Load Duration = ' + CAST(datediff(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------';

		SET @START_TIME = GETDATE();

		PRINT 'Truncating table silver.erp_loc_a101';

		TRUNCATE TABLE silver.erp_loc_a101;

		PRINT 'Loading >> silver.erp_loc_a101';

		INSERT INTO silver.erp_loc_a101 (
			cid,
			cntry
			)
		SELECT trim(replace(cid, '-', '')) AS cid,
			CASE 
				WHEN upper(trim(cntry)) = 'DE'
					THEN 'Germany'
				WHEN upper(trim(cntry)) IN (
						'US',
						'USA'
						)
					THEN 'United States'
				WHEN cntry IS NULL OR trim(cntry) = ''
					THEN 'Unknown'
				ELSE trim(cntry)
				END AS cntry
		FROM bronze.erp_loc_a101;

		PRINT 'Completed >> silver.erp_loc_a101';

		SET @END_TIME = GETDATE();

		PRINT 'Load Duration = ' + CAST(datediff(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------';

		SET @START_TIME = GETDATE();

		PRINT 'Truncating table silver.erp_px_cat_g1v2';

		TRUNCATE TABLE silver.erp_px_cat_g1v2;

		PRINT 'Loading >> silver.erp_px_cat_g1v2';

		INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
			)
		SELECT id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2

		PRINT 'Completed >> silver.erp_px_cat_g1v2';

		SET @END_TIME = GETDATE();

		PRINT 'Load Duration = ' + CAST(datediff(SECOND, @START_TIME, @END_TIME) AS NVARCHAR) + ' seconds';
		PRINT '------------------------------------';
		PRINT '==============================================';
		PRINT 'Completed Loading Silver Layer';
		PRINT '==============================================';

		SET @BATCH_END = GETDATE();

		PRINT 'Total time to complete execution of load_silver = ' + CAST(datediff(second, @BATCH_START, @BATCH_END) AS VARCHAR) + ' seconds';
	END TRY

	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'Error occurred';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================';
	END CATCH
END