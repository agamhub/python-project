CREATE PROC [FOND_ID].[USP_LOAD_ETL5_GA_ALLOWANCE_DRIVER_DETAIL_YTD] @batch [NVARCHAR](100) AS
BEGIN 
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_ETL5_GA_ALLOWANCE_DRIVER_DETAIL';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @drivername NVARCHAR(15) = 'GAALLOWANCE';
	

	BEGIN TRY
	
		SET @V_START_DATE	= convert(date, cast(@batch as varchar)); -- valuation extract date
		PRINT	'Start date :' + convert(varchar,@V_START_DATE,112);
		SET @V_START 	= convert(datetime,getDATE());

		SET @V_DESCRIPTION 	= 'Start ' + @V_FUNCTION_NAME + ' : ' + convert(varchar,@V_START,121);
		PRINT	@V_DESCRIPTION;
		SET @V_SEQNO		= @V_SEQNO + 1;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
		---------------------------- DROP TEMPORARY TABLE ------------------------------
		IF OBJECT_ID('tempdb..#etl5_ga_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_ga_driver
		END;

		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE etl5_ga_driver : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);		
	
		--SELECT YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		
		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
		--DECLARE @batch [nvarchar](30);
		--SET @batch = '20190101';
		--DECLARE @drivername NVARCHAR(15) = 'GAALLOWANCE';
	
		SELECT * 
		INTO #etl5_ga_driver
		FROM (
		SELECT 
				'IAI' ENTITY_ID,
				'LifeAsia' DRIVER_SOURCE,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DRIVER_PERIOD,
				[POLICY_NO] as POL_NO,
				max([BENF_CD]) as BENF_CD,
				max([PROD_CD]) as PROD_CD,
				'' [TREATY_CD],
				max([ADJ_T0]) AS FUND,
				@drivername ALLOCATION_DRIVER,
				SUM(GA_ALLOWANCE) AS DRIVER_AMOUNT,
				'0' BATCH_MASTER_ID,
				'0' BATCH_RUN_ID,
				'0' JOB_MASTER_ID,
				'0' JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
		FROM FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_GA_ALLOWANCE
		--[FOND_ID].[FOND_LIFEASIA_GA_ALLOWANCE_DRIVER_DETAIL]
		WHERE   ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		group by [POLICY_NO]
		)a
		;
	
		-- SOURCE : select top 10 * from FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_GA_ALLOWANCE
		-- TARGET : select top 10 * from FOND_ID.FOND_ETL5_GA_ALLOWANCE_DRIVER_DETAIL
		
		--DECLARE @batch [nvarchar](30);
		--SET @batch = '20190101';
		--DECLARE @drivername NVARCHAR(15) = 'GAALLOWANCE';
		-- DELETE EXISTING DATA
		DELETE FROM FOND_ID.FOND_ETL5_GA_ALLOWANCE_DRIVER_DETAIL
		WHERE DRIVER_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)));
		
	
		---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.FOND_ETL5_GA_ALLOWANCE_DRIVER_DETAIL : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

	
		INSERT INTO FOND_ID.FOND_ETL5_GA_ALLOWANCE_DRIVER_DETAIL
		SELECT * FROM #etl5_ga_driver;
			
       	---------------------------- ETL5 LOGGING ----------------------------   
       	--DECLARE @batch [nvarchar](30);
		--SET @batch = '20190101';
		--DECLARE @drivername NVARCHAR(15) = 'GAALLOWANCE';
	
		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#etl5_ga_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_ga_driver
		END;  
	
	
	END TRY
	
	BEGIN CATCH
		IF @@TRANCOUNT > 0
                ROLLBACK;
	    SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_END 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	='Error execution for function on ' 
							+ @V_FUNCTION_NAME + ' at ' + convert(varchar,@V_START,121) 
							+ ' with Error Message : ' + ERROR_MESSAGE();
		PRINT @V_DESCRIPTION;
		
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION) VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
		raiserror(@V_DESCRIPTION, 18, 1)
	END CATCH

END;

