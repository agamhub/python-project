CREATE PROC [FOND_ID].[USP_LOAD_ETL5_FYP_PAA_DRIVER_DETAIL] @batch [nvarchar](30),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_ETL5_DPAS_DRIVER_DETAIL';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @drivername NVARCHAR(15);
	SET @drivername = 'FYP_PAA';
	
	------START GET RUN ID DETAIL FROM ABC------
	DECLARE 
	@BATCH_MASTER_ID  VARCHAR(20) = 0,
	@BATCH_RUN_ID    VARCHAR(20) = 0,
	@JOB_MASTER_ID   VARCHAR(20) = 0,
	@JOB_RUN_ID     VARCHAR(20) = 0,
	@GMT_START_DTTM   VARCHAR(19) = CONVERT(DATETIME2, GETDATE());

	EXEC STAG_ID.USP_GetRunIdReturn
	@JobName     = @JOBNAMESTR,
	@BATCH_MASTER_ID = @BATCH_MASTER_ID OUTPUT,
	@BATCH_RUN_ID  = @BATCH_RUN_ID OUTPUT,
	@JOB_MASTER_ID  = @JOB_MASTER_ID OUTPUT,
	@JOB_RUN_ID   = @JOB_RUN_ID OUTPUT,
	@GMT_START_DTTM = @GMT_START_DTTM OUTPUT;
	------END GET RUN ID DETAIL FROM ABC------
	
	BEGIN TRY

	
		
		SET @V_START_DATE	= convert(date, cast(@batch as varchar(8))); -- valuation extract date
		PRINT	'Start date :' + convert(varchar,@V_START_DATE,112);
		SET @V_START 	= convert(datetime,getDATE());

		SET @V_DESCRIPTION 	= 'Start ' + @V_FUNCTION_NAME + ' : ' + convert(varchar,@V_START,121);
		PRINT	@V_DESCRIPTION;
		SET @V_SEQNO		= @V_SEQNO + 1;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

		---------------------------- DROP TEMPORARY TABLE ------------------------------
		IF OBJECT_ID('tempdb..#etl5_fyp_paa_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_fyp_paa_driver
		END;

		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TEMPORARY TABLE etl5_fyp_paa_driver : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
	
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

		select
		  ENTITY_ID,
		  DRIVER_SOURCE,
		  ACCOUNTING_PERIOD,
	      POL_NO,
	      BENF_CD,
	      PROD_CD,
		  '' AS TREATY_CD,
	      FUND AS ADJ_T0,
		  FYP_AMOUNT
		into #etl5_fyp_paa_driver
		FROM STAG_ID.STAG_CONFIG_ETL5_IFRS17_DPAS_MAPPING

	
		---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.FOND_ETL5_DPAS_DRIVER_DETAIL : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
		
		DELETE FROM FOND_ID.FOND_ETL5_DPAS_DRIVER_DETAIL WHERE left(convert(varchar,BATCHDATE,112),6) = left(@batch,6);
			
		---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.FOND_ETL5_DPAS_DRIVER_DETAIL : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

		INSERT INTO FOND_ID.FOND_ETL5_DPAS_DRIVER_DETAIL
		(
		  ENTITY_ID,
		  DRIVER_SOURCE,
		  DRIVER_PERIOD,
		  --TRANSACTION_DATE,
		  POL_NO,
		  BENF_CD,
		  PROD_CD,
		  TREATY_CD,
		  ADJ_T0,
		  DRIVER_AMOUNT,
		  BATCH_MASTER_ID,
		  BATCH_RUN_ID,
		  JOB_MASTER_ID,
		  JOB_RUN_ID,
		  BATCHDATE,
		  ETL_PROCESS_DATE_TIME
		)
		
		SELECT	
		  ENTITY_ID,
		  DRIVER_SOURCE,
		  ACCOUNTING_PERIOD,
		  --TRANSACTION_DATE,
		  POL_NO,
		  BENF_CD,
		  PROD_CD,
		  TREATY_CD,
		  ADJ_T0,
		  FYP_AMOUNT,
		  @BATCH_MASTER_ID,
		  @BATCH_RUN_ID,
		  @JOB_MASTER_ID,
		  @JOB_RUN_ID,
		  CAST(@batch AS NVARCHAR(6)) as BATCHDATE,
		  GETDATE() as ETL_PROCESS_DATE_TIME
		FROM #etl5_fyp_paa_driver;
		
		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #etl5_fyp_paa_driver) ;
	    SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))
	
		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_DPAS_DRIVER_DETAIL'
		,@drivername,@V_TOTAL_ROWS,'MTD',@V_PERIOD);
		
		IF @@TRANCOUNT > 0
            COMMIT;
		
		---------------------------- INSERT INTO DATA INTO TARGET TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#etl5_fyp_paa_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_fyp_paa_driver
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
