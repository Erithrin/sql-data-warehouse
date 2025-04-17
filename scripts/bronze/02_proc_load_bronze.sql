/*
=====================================================================
Stored Procedure : Loads bronze layer (Source ---> Bronze)

Script Purpose :
This procedure loads data from CRM and ERP sources into the bronze tables. The source data is in csv files.

It performs the following actions :
1. Truncates bronze tables before loading the data
2. Performs bulk insert to load the data from csv files to bronze tables

Parameter: None. This procedure neither accepts any parameters nor does it return any value.

Usage : exec bronze.load_bronze
*/

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = getdate();
		print '====================================================';
		print 'Starting to execute bronze.load_bronze';
		print '====================================================';
		set @start_time = getdate();

		print '----------------------------------------------------';
		print 'Beginning to load CRM data';
		print '----------------------------------------------------';

		print 'Truncating table >> bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print 'Insert into table >> bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'D:\Professional Certificate for Data Analyst COurse\Projects\DataWarehouse project\sql-data-warehouse\datasets\source_crm\cust_info.csv'
		with 
		(
		firstrow = 2,
		fieldterminator = ',',
		rowterminator = '\n',
		tablock
		);
		set @end_time = getdate();
		print 'Load time taken for bronze.crm_cust_info = '+ cast(datediff(second, @start_time, @end_time) as nvarchar)+' seconds';
		print 'Completed >> bronze.crm_cust_info';
		print '----------------------------------------------------';


		set @start_time = getdate();
		print 'Truncating table >> bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		print 'Insert into table >> bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'D:\Professional Certificate for Data Analyst COurse\Projects\DataWarehouse project\sql-data-warehouse\datasets\source_crm\prd_info.csv'
		with 
		(
		firstrow = 2,
		fieldterminator = ',',
		rowterminator = '\n',
		tablock
		);
		set @end_time = getDate();
		print 'Load time taken for bronze.crm_prd_info = '+ cast(datediff(second, @start_time, @end_time) as varchar) + ' seconds';
		print 'Completed >> bronze.crm_prd_info';
		print '----------------------------------------------------';

		set @start_time = getdate();
		print 'Truncating table >> bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		print 'Insert into table >> bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'D:\Professional Certificate for Data Analyst COurse\Projects\DataWarehouse project\sql-data-warehouse\datasets\source_crm\sales_details.csv'
		with 
		(
		firstrow = 2,
		fieldterminator = ',',
		rowterminator = '\n',
		tablock
		);
		set @end_time = getdate();
		print 'Load time taken for bronze.crm_sales_details = '+ cast(datediff(second, @start_time, @end_time) as nvarchar) +' seconds';
		print 'Completed >> bronze.crm_prd_info';
		print '----------------------------------------------------';



		print '----------------------------------------------------';
		print 'Beginning to load ERP data';
		print '----------------------------------------------------';



		set @start_time = getdate();
		print 'Truncating table >> bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		print 'Insert into table >> bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'D:\Professional Certificate for Data Analyst COurse\Projects\DataWarehouse project\sql-data-warehouse\datasets\source_erp\CUST_AZ12.csv'
		with 
		(
		firstrow = 2,
		fieldterminator = ',',
		rowterminator = '\n',
		tablock
		);
		set @end_time = getdate();
		print 'Load time taken for bronze.erp_cust_az12 = ' + cast(datediff(second, @start_time, @end_time) as nvarchar) +' seconds';
		print 'Completed >> bronze.erp_cust_az12';
		print '----------------------------------------------------';

		set @start_time = getdate();
		print 'Truncating table >> bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		print 'Insert into table >> bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'D:\Professional Certificate for Data Analyst COurse\Projects\DataWarehouse project\sql-data-warehouse\datasets\source_erp\loc_a101.csv'
		with 
		(
		firstrow = 2,
		fieldterminator = ',',
		rowterminator = '\n',
		tablock
		);
		set @end_time = getdate();
		print 'Load time taken for bronze.erp_loc_a101 = '+ cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print 'Completed >> bronze.erp_loc_a101';
		print '----------------------------------------------------';


		set @start_time = getdate();
		print 'Truncating table >> bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		print 'Insert into table >> bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'D:\Professional Certificate for Data Analyst COurse\Projects\DataWarehouse project\sql-data-warehouse\datasets\source_erp\PX_CAT_G1V2.csv'
		with 
		(
		firstrow = 2,
		fieldterminator = ',',
		rowterminator = '\n',
		tablock
		);
		set @end_time = getdate();
		print 'Load time taken for bronze.erp_px_cat_g1v2 = '+cast(datediff(second, @start_time, @end_time) as nvarchar) +' seconds';
		print 'Completed >> bronze.erp_px_cat_g1v2';
		print '----------------------------------------------------';

		set @batch_end_time = getdate();
		print 'Load time taken for batch to complete =' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds';

		print '====================================================';
		print 'Completed execution of bronze.load_bronze';
		print '====================================================';

	
	end try
	begin catch
		print '====================================================';
		print 'Error Occurred';
		print 'Error Message ' + error_message();
		print 'Error Number ' + cast(error_number() as nvarchar);
		print 'Error State ' + cast(error_state() as nvarchar);
		print '====================================================';
	end catch
end