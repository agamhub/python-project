CREATE PROC [FOND_ID].[USP_LOAD_ETL5_PRUEMAS_FYP_DRIVER_DETAIL] @batch [nvarchar](30) AS
BEGIN 
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_ETL5_PRUEMAS_FYP_DRIVER_DETAIL';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE	@EXTRACT_YEAR VARCHAR(4);
	DECLARE	@EXTRACT_MONTH VARCHAR(2);
	
	
	BEGIN TRY

		
		SET @V_START_DATE	= convert(date, cast(@batch as varchar(8))); -- valuation extract date
		PRINT	'Start date :' + convert(varchar,@V_START_DATE,112);
		SET @V_START 	= convert(datetime,getDATE());

		SET @V_DESCRIPTION 	= 'Start ' + @V_FUNCTION_NAME + ' : ' + convert(varchar,@V_START,121);
		PRINT	@V_DESCRIPTION;
		SET @V_SEQNO		= @V_SEQNO + 1;
		
		SET	@EXTRACT_YEAR = YEAR(CONVERT(varchar(8),@batch));
		PRINT @EXTRACT_YEAR;
	
		-- Extracting Parameter Input Month
		SET @EXTRACT_MONTH = MONTH(CONVERT(varchar(8),@batch));
		PRINT @EXTRACT_MONTH;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
		---------------------------- DROP TEMPORARY TABLE ------------------------------
		IF OBJECT_ID('tempdb..#etl5_pruemas_fyp_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_pruemas_fyp_driver
		END;


		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TEMPORARY TABLE etl5_pruemas_fyp_driver : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
	
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
		
		SELECT
			YEAR(DATEADD(month, 0,UPLOAD_DATE)) * 1000 + 0 + MONTH(DATEADD(month, 0,UPLOAD_DATE))  ACCOUNTING_PERIOD,
			CAST((DATEADD(ms,-2,DATEADD(month, DATEDIFF(month, 0, GETDATE()), 0))) AS DATE) as TRANSACTION_DATE,
			cast(ID as varchar(20)) as POL_NO,
			'PruEmas' as BENF_CD,
			'PruEmas' as PROD_CD,
			'GTNN000' as ADJ_TO,
			'101000' as DIST_CHAN,
			PREMIUM as GROSS_PREMIUM,
			YTD as GROSS_PREMIUM_YTD,
			0 APE,
			count(*) over (partition by UPLOAD_DATE) as PCNT_GROSS,
			PREMIUM as RENEWAL_PREMIUM,
			BENEFIT as PER_SUM_ASSURED,
			0 as NOP_NB,
			0 as PER_ACCOUNT_VALUE,
			PREMIUM_PER_MONTH as COLLECTED_PREMIUM,
			BATCH_MASTER_ID,
			BATCH_RUN_ID,
			JOB_MASTER_ID,
			JOB_RUN_ID,
			Cast(@batch AS NVARCHAR(6)) as BATCHDATE,
			GETDATE() ETL_PROCESS_DATE_TIME
		INTO #etl5_pruemas_fyp_driver
		FROM [STAG_ID].[STAG_PRUEMAS_STAG_ETL4_PREMIUM]
		where YEAR(UPLOAD_DATE) = @EXTRACT_YEAR AND MONTH(UPLOAD_DATE) = @EXTRACT_MONTH;
		
		SELECT * FROM #etl5_pruemas_fyp_driver
		
		---------------------------- TO Handle rerun process ------------------------------
			BEGIN TRANSACTION;
			SET @V_SEQNO 	= @V_SEQNO + 1;
			SET @V_START 	= convert(datetime,getDATE());
			SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.FOND_ETL5_PRUEMAS_FYP_DRIVER_DETAIL : ' + convert(varchar,@V_START,121);
			PRINT @V_DESCRIPTION;
		
			INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
			VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

			DELETE FROM FOND_ID.FOND_ETL5_PRUEMAS_FYP_DRIVER_DETAIL 
			WHERE ACCOUNTING_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)));
			

			
			---------------------------- TO Handle rerun process ------------------------------
			SET @V_SEQNO 	= @V_SEQNO + 1;
			SET @V_START 	= convert(datetime,getDATE());
			SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.FOND_ETL5_PRUEMAS_FYP_DRIVER_DETAIL : ' + convert(varchar,@V_START,121);
			PRINT @V_DESCRIPTION;
		
			INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
			VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
			
			INSERT INTO FOND_ID.FOND_ETL5_PRUEMAS_FYP_DRIVER_DETAIL
			SELECT * FROM #etl5_pruemas_fyp_driver;
			
			IF @@TRANCOUNT > 0
                COMMIT;
		  
		  
		  
		---------------------------- INSERT INTO DATA INTO TARGET TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#etl5_pruemas_fyp_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_pruemas_fyp_driver
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

