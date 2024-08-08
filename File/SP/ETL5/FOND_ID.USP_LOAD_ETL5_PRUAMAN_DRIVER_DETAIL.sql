CREATE PROC [FOND_ID].[USP_LOAD_ETL5_PRUAMAN_DRIVER_DETAIL] @batch [nvarchar](30),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.USP_LOAD_ETL5_PRUAMAN_DRIVER_DETAIL';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @drivername NVARCHAR(15);
	SET @drivername = 'PRUAMAN';
	
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
		IF OBJECT_ID('tempdb..#etl5_pruaman_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_pruaman_driver
		END;

		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TEMPORARY TABLE etl5_pruaman_driver : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
	
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

		select
		  PRUAMAN.ACCT_PERIOD,
		  PRUAMAN.POLICY_NO,
		  PRUAMAN.AMT_RPT_CCY,
		  PRUAMAN_YTD.YTD_AMT_RPT_CCY
		into #etl5_pruaman_driver
		from 
		  ( 
		    select
			  ACCT_PERIOD,
			  POLICY_NO,
			  AMT_RPT_CCY
			from FOND_ID.FOND_PRUAMAN_ETL4_PRUAMAN
            where ACCT_PERIOD = CONCAT(LEFT(@batch, 4), '0', SUBSTRING(@batch, 5,2))
		    and SUBSTRING(REPLACE(TXN_SK,'-',''),8,2) = '03'
            and SUN_CD = '4111100100'
		  ) as PRUAMAN
		
		left join
		  (
		    select
			  POLICY_NO,
			  sum(AMT_RPT_CCY) as YTD_AMT_RPT_CCY
			from FOND_ID.FOND_PRUAMAN_ETL4_PRUAMAN
            where LEFT(ACCT_PERIOD, 4) = LEFT(@batch, 4)
		    and SUBSTRING(REPLACE(TXN_SK,'-',''),8,2) = '03'
            and SUN_CD = '4111100100'
			group by POLICY_NO
		  ) as PRUAMAN_YTD
		on PRUAMAN.POLICY_NO = PRUAMAN_YTD.POLICY_NO;

		---------------------------- TO Handle rerun process ------------------------------
			BEGIN TRANSACTION;
			SET @V_SEQNO 	= @V_SEQNO + 1;
			SET @V_START 	= convert(datetime,getDATE());
			SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.FOND_ETL5_PRUAMAN_DRIVER_DETAIL : ' + convert(varchar,@V_START,121);
			PRINT @V_DESCRIPTION;
		
			INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
			VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

			DELETE FROM FOND_ID.FOND_ETL5_PRUAMAN_DRIVER_DETAIL 
			WHERE ACCOUNTING_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)));
			
			DELETE FROM FOND_ID.FOND_ETL5_PRUAMAN_FYP_DRIVER_DETAIL 
			WHERE ACCOUNTING_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)));
			
			
			---------------------------- TO Handle rerun process ------------------------------
			SET @V_SEQNO 	= @V_SEQNO + 1;
			SET @V_START 	= convert(datetime,getDATE());
			SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.FOND_ETL5_PRUAMAN_DRIVER_DETAIL : ' + convert(varchar,@V_START,121);
			PRINT @V_DESCRIPTION;
		
			INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
			VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
			
			INSERT INTO FOND_ID.FOND_ETL5_PRUAMAN_DRIVER_DETAIL
			SELECT
		      ACCT_PERIOD AS ACCOUNTING_PERIOD,
		      CAST(DATEADD(day, -1, DATEADD(month, 1, @batch)) AS DATE) as TRANSACTION_DATE,
		      POLICY_NO as POL_NO,
		      'PruAman' as BENF_CD,
		      'PruAman' as PROD_CD,
		      'GTNN000' as ADJ_TO,
		      null as DIST_CHAN,
		      AMT_RPT_CCY as GROSS_PREMIUM,
		      YTD_AMT_RPT_CCY as GROSS_PREMIUM_YTD,
		      AMT_RPT_CCY/10 as APE,
		      1 as PCNT_GROSS,
		      --PREMIUM_GROSS as RENEWAL_PREMIUM,
		      0 as RENEWAL_PREMIUM,
		      NULL as PER_SUM_ASSURED,
		      1 as NOP_NB,
		      0 as PER_ACCOUNT_VALUE,
		      AMT_RPT_CCY as COLLECTED_PREMIUM,
		      @BATCH_MASTER_ID AS BATCH_MASTER_ID,
			  @BATCH_RUN_ID AS BATCH_RUN_ID,
			  @JOB_MASTER_ID AS JOB_MASTER_ID,
			  @JOB_RUN_ID AS JOB_RUN_ID,
		      left(replace(Cast(@batch AS NVARCHAR(12)),'-',''),6) as DL_PLAI_BATCHDATE,
		      GETDATE() ETL_PROCESS_DATE_TIME
			FROM #etl5_pruaman_driver;

			INSERT INTO FOND_ID.FOND_ETL5_PRUAMAN_FYP_DRIVER_DETAIL
			SELECT
		      ACCT_PERIOD AS ACCOUNTING_PERIOD,
		      CAST(DATEADD(day, -1, DATEADD(month, 1, @batch)) AS DATE) as TRANSACTION_DATE,
		      --a.CONTRACT_NUMBER as POL_NO,
		      POLICY_NO as POL_NO,
		      'PruAman' as BENF_CD,
		      'PruAman' as PROD_CD,
		      'GTNN000' as ADJ_TO,
		      null as DIST_CHAN,
		      AMT_RPT_CCY as GROSS_PREMIUM,
		      YTD_AMT_RPT_CCY as GROSS_PREMIUM_YTD,
		      AMT_RPT_CCY/10 as APE,
		      1 as PCNT_GROSS,
		      --PREMIUM_GROSS as RENEWAL_PREMIUM,
		      0 as RENEWAL_PREMIUM,
		      NULL as PER_SUM_ASSURED,
		      1 as NOP_NB,
		      0 as PER_ACCOUNT_VALUE,
		      AMT_RPT_CCY as COLLECTED_PREMIUM,
		      AMT_RPT_CCY as FYP,
		      @BATCH_MASTER_ID AS BATCH_MASTER_ID,
			  @BATCH_RUN_ID AS BATCH_RUN_ID,
			  @JOB_MASTER_ID AS JOB_MASTER_ID,
			  @JOB_RUN_ID AS JOB_RUN_ID,
		      left(replace(Cast(@batch AS NVARCHAR(12)),'-',''),6) as DL_PLAI_BATCHDATE,
		      GETDATE() ETL_PROCESS_DATE_TIME
		    FROM #etl5_pruaman_driver;
		
			---------------------------- ETL5 LOGGING ----------------------------      
       	
			DECLARE @V_TOTAL_ROWS integer = 0;
			DECLARE @V_PERIOD nvarchar(10);
			SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #etl5_pruaman_driver) ;
	        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))
	
			INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
			VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_PRUAMAN_DRIVER_DETAIL'
			,@drivername,@V_TOTAL_ROWS,'MTD',@V_PERIOD);
			
			SELECT 'Total records : ' + @V_PERIOD;
			
			IF @@TRANCOUNT > 0
                COMMIT;
		  
		  
		  
		---------------------------- INSERT INTO DATA INTO TARGET TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#etl5_pruaman_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_pruaman_driver
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
