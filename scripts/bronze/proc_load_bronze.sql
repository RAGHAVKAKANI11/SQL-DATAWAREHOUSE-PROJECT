

CREATE OR ALTER procedure bronze.load_bronze as 
begin
declare @start_time datetime , @end_time datetime,@batch_start_time DATETIME, @batch_end_time DATETIME;
	begin try
	    SET @batch_start_time=getdate();
		print '=============================================';
		print 'Loading Bronze Layer';
		print '=============================================';

		print '---------------------------------------------';
		print 'Loading CRM Tables';
		print '---------------------------------------------';

		set @start_time=getdate();
		print '>>Truncating Table:bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;
		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\kakan\OneDrive - Manipal University Jaipur\Desktop\SQL PROJECTS\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
			);
		set @end_time=GETDATE();
		print '>>LoadDuration: '+ cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds';
		print '----------------------';

		set @start_time=getdate();
		print '>>Truncating Table:bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;
		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\kakan\OneDrive - Manipal University Jaipur\Desktop\SQL PROJECTS\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
			);
		set @end_time=GETDATE();
		print '>>LoadDuration: '+ cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds';
		print '----------------------';

        set @start_time=getdate();
		print '>>Truncating Table:bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;
		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\kakan\OneDrive - Manipal University Jaipur\Desktop\SQL PROJECTS\sql-data-warehouse-project\datasets\source_crm\sales_Details.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
			);
		set @end_time=GETDATE();
		print '>>LoadDuration: '+ cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds';
		print '----------------------';

		print '---------------------------------------------';
		print 'Loading ERP Tables';
		print '---------------------------------------------';

		set @end_time=GETDATE();
		print '>>Truncating Table:bronze.erp_cust_az12';
		truncate table bronze.erp_cust_az12;
		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		bulk insert bronze.erp_cust_az12
		from 'C:\Users\kakan\OneDrive - Manipal University Jaipur\Desktop\SQL PROJECTS\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
			);
		set @end_time=GETDATE();
		print '>>LoadDuration: '+ cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds';
		print '----------------------';

		set @end_time=GETDATE();
		print '>>Truncating Table:bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;
		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\kakan\OneDrive - Manipal University Jaipur\Desktop\SQL PROJECTS\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
			);
		set @end_time=GETDATE();
		print '>>LoadDuration: '+ cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds';
		print '----------------------';

		set @end_time=GETDATE();
		print '>>Truncating Table:bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;
		print ' >>Inserting Data Into :bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\kakan\OneDrive - Manipal University Jaipur\Desktop\SQL PROJECTS\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		with (
			firstrow=2,
			fieldterminator=',',
			tablock
			);
		set @end_time=GETDATE();
		print '>>LoadDuration: '+ cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds';
		print '----------------------';


		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Bronze Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='

	end try 
	begin catch
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	end catch
end


