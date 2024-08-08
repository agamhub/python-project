CREATE PROC [FOND_ID].[USP_LOAD_ETL5_PAYLIFE_DRIVER_DETAIL] @batch [nvarchar](30),@JOBNAMESTR [NVARCHAR](2000) AS 
BEGIN 
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.USP_LOAD_ETL5_PAYLIFE_DRIVER_DETAIL';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	
	

--=======================================
--For Debuging Process
--DECLARE @date date = '2019-11-22'
--=======================================
	DECLARE @trx_date DATE = cast(@batch as date)
	DECLARE @extraction_dt varchar(10)=convert(varchar(10),dateadd(month,-1,CONVERT(date,@batch)),112)

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

		IF OBJECT_ID('tempdb..#etl5_paylife_driver_detail') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_paylife_driver_detail
		END;

		IF OBJECT_ID('tempdb..#etl5_paylife_fyp_driver_detail') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_paylife_fyp_driver_detail
		END;


		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE etl5_paylife_driver_detail : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

		WITH step1 AS
		  (
		    SELECT
			  DATEADD(MONTH, 1, DATEADD(MONTH, DATEDIFF(MONTH, 0, UPLOAD_DATE), 0)) AS ACCOUNTING_PERIOD,
			  convert(varchar,DATEADD(month, ((YEAR(dateadd(month,1,UPLOAD_DATE)) - 1900) * 12) + MONTH(dateadd(month,1,UPLOAD_DATE)), -1),23) AS [TRANSACTION_DATE], 
			  Cast(INSURED_ID AS VARCHAR(36)) AS [POL_NO], 
			  'PayLife'                      AS [BENF_CD], 
			  'PayLife'                      AS [PROD_CD], 
			  'GTNN000'                      AS [ADJ_TO], 
			  '206128'                       AS [DIST_CHAN], 
			  --1000 * count(INSURED_ID) over (partition by COMPANY_CODE) as [GROSS_PREMIUM], 
			  Cast('1000' AS FLOAT) * -1     AS [GROSS_PREMIUM], 
			  Cast('1000' AS BIGINT)         AS [APE], 
			  Cast('1' AS BIGINT)            AS [PCNT_GROSS], 
			  --Cast('0' AS BIGINT)            AS [RENEWAL_PREMIUM], 
			  (case when [INSURED_TYPE] = 'New Business' THEN Cast('0' AS BIGINT) else Cast('1000' AS FLOAT) end) * -1 AS [RENEWAL_PREMIUM],
			  case when [INSURED_TYPE] = 'New Business' THEN Cast('12500000' AS BIGINT)
			  else 0 end   AS [PER_SUM_ASSURED], 
			  CASE 
			    WHEN [INSURED_TYPE] like 'New%' THEN Cast('1' AS BIGINT)
			    WHEN [INSURED_TYPE] like 'Renewa%' THEN Cast('0' AS BIGINT) 
			    ELSE Cast('0' AS BIGINT) 
			  END                            AS [NOP_NB], 
			  	Cast('0' AS BIGINT)            AS [PER_ACCOUNT_VALUE], 
			  	Cast('1000' AS FLOAT) * -1     AS [COLLECTED_PREMIUM], 
			  	@BATCH_MASTER_ID AS [DL_PLAI_BATCHID],
				@BATCH_RUN_ID AS [DL_PLAI_BATCH_RUN_ID],
				@JOB_MASTER_ID AS [DL_PLAI_JOBID],
				@JOB_RUN_ID AS [DL_PLAI_JOB_RUN_ID], 
			  	Cast(@batch AS NVARCHAR(6))       AS [DL_PLAI_BATCHDATE], 
			  	Getdate()                      AS [ETL_PROCESS_DATE_TIME], 
			  	UPLOAD_DATE, 
			  	INSURED_ID 
			FROM STAG_ID.STAG_PAYLIFE_STAG_TBIILIFE_INSURED_FINAL 
			--WHERE YEAR(DATEADD(MONTH,1,CAST(UPLOAD_DATE AS DATE))) = CAST(LEFT(@batch,4) as integer)
			WHERE DATEADD(MONTH, 1, DATEADD(MONTH, DATEDIFF(MONTH, 0, UPLOAD_DATE), 0)) = @batch
		  ), 
				 
		step2 AS
		  (
		    SELECT
			  @trx_date AS ACCOUNTING_PERIOD, 
			  INSURED_ID,
			  COUNT(*) * -1000 AS GROSS_PREMIUM_YTD
			FROM STAG_ID.STAG_PAYLIFE_STAG_TBIILIFE_INSURED_FINAL
			WHERE YEAR(DATEADD(MONTH, 1, DATEADD(MONTH, DATEDIFF(MONTH, 0, UPLOAD_DATE), 0))) = YEAR(@batch)
			AND DATEADD(MONTH, 1, UPLOAD_DATE) <= @batch
			GROUP  BY INSURED_ID
		  ), 
		
		step3 AS
		  (
		    SELECT
			  t1.*, 
			  t2.GROSS_PREMIUM_YTD
			FROM step1 AS t1 
			LEFT JOIN step2 AS t2 
			ON t1.INSURED_ID = t2.INSURED_ID 
			AND t1.ACCOUNTING_PERIOD = t2.ACCOUNTING_PERIOD
		  )

		-------------------------------INSERT TO TMP----------------------------------
		SELECT *
		INTO #etl5_paylife_driver_detail
		FROM
		  (
		    SELECT
		      CONCAT(YEAR(ACCOUNTING_PERIOD), FORMAT(MONTH(ACCOUNTING_PERIOD), '000')) AS [ACCOUNTING_PERIOD],
		      [TRANSACTION_DATE], 
		      [POL_NO], 
		      [BENF_CD], 
		      [PROD_CD], 
		      [ADJ_TO], 
		      [DIST_CHAN], 
		      [GROSS_PREMIUM], 
		      [GROSS_PREMIUM_YTD], 
		      [APE], 
		      [PCNT_GROSS], 
		      [RENEWAL_PREMIUM], 
		      [PER_SUM_ASSURED], 
		      [NOP_NB], 
		      [PER_ACCOUNT_VALUE], 
		      [COLLECTED_PREMIUM], 
		      [DL_PLAI_BATCHID], 
		      [DL_PLAI_BATCH_RUN_ID], 
		      [DL_PLAI_JOBID], 
		      [DL_PLAI_JOB_RUN_ID], 
		      [DL_PLAI_BATCHDATE], 
		      [ETL_PROCESS_DATE_TIME] 
		    FROM step3 
          ) a; 


		WITH step1 AS
		  (
		    SELECT
			  DATEADD(MONTH, 1, DATEADD(MONTH, DATEDIFF(MONTH, 0, UPLOAD_DATE), 0)) AS ACCOUNTING_PERIOD,
			  convert(varchar,DATEADD(month, ((YEAR(dateadd(month,1,UPLOAD_DATE)) - 1900) * 12) + MONTH(dateadd(month,1,UPLOAD_DATE)), -1),23) AS [TRANSACTION_DATE], 
			  Cast(INSURED_ID AS VARCHAR(36)) AS [POL_NO], 
			  'PayLife'                      AS [BENF_CD], 
			  'PayLife'                      AS [PROD_CD], 
			  'GTNN000'                      AS [ADJ_TO], 
			  '206128'                       AS [DIST_CHAN], 
			  --1000 * count(INSURED_ID) over (partition by COMPANY_CODE) as [GROSS_PREMIUM], 
			  Cast('1000' AS FLOAT) * -1     AS [GROSS_PREMIUM], 
			  Cast('1000' AS BIGINT)         AS [APE], 
			  Cast('1' AS BIGINT)            AS [PCNT_GROSS], 
			  --Cast('0' AS BIGINT)            AS [RENEWAL_PREMIUM], 
			  (case when [INSURED_TYPE] = 'New Business' THEN Cast('0' AS BIGINT) else Cast('1000' AS FLOAT) end) * -1 AS [RENEWAL_PREMIUM],
			  case when [INSURED_TYPE] = 'New Business' THEN Cast('12500000' AS BIGINT)
			  else 0 end   AS [PER_SUM_ASSURED], 
			  CASE 
			    WHEN [INSURED_TYPE] like 'New%' THEN Cast('1' AS BIGINT)
			    WHEN [INSURED_TYPE] like 'Renewa%' THEN Cast('0' AS BIGINT) 
			    ELSE Cast('0' AS BIGINT) 
			  END                            AS [NOP_NB], 
			  Cast('0' AS BIGINT)            AS [PER_ACCOUNT_VALUE], 
			  Cast('1000' AS FLOAT) * -1     AS [COLLECTED_PREMIUM], 
				@BATCH_MASTER_ID AS [DL_PLAI_BATCHID],
				@BATCH_RUN_ID AS [DL_PLAI_BATCH_RUN_ID],
				@JOB_MASTER_ID AS [DL_PLAI_JOBID],
				@JOB_RUN_ID AS [DL_PLAI_JOB_RUN_ID],  
			  Cast(@batch AS NVARCHAR(6))       AS [DL_PLAI_BATCHDATE], 
			  Getdate()                      AS [ETL_PROCESS_DATE_TIME], 
			  UPLOAD_DATE, 
			  INSURED_ID
			FROM STAG_ID.STAG_PAYLIFE_STAG_TBIILIFE_INSURED_FINAL 
			WHERE DATEADD(MONTH, 1, DATEADD(MONTH, DATEDIFF(MONTH, 0, UPLOAD_DATE), 0)) = @batch
			--AND INSURED_TYPE = 'New Business'
		), 

		step2 AS
		  (
		    SELECT
			  @trx_date AS ACCOUNTING_PERIOD, 
			  INSURED_ID,
			  COUNT(*) * -1000 AS GROSS_PREMIUM_YTD
			FROM STAG_ID.STAG_PAYLIFE_STAG_TBIILIFE_INSURED_FINAL
			WHERE YEAR(DATEADD(MONTH, 1, DATEADD(MONTH, DATEDIFF(MONTH, 0, UPLOAD_DATE), 0))) = YEAR(@batch)
			AND DATEADD(MONTH, 1, UPLOAD_DATE) <= @batch
			GROUP  BY INSURED_ID
		  ), 
		
		step3 AS
		  (
		    SELECT
			  t1.*, 
			  t2.GROSS_PREMIUM_YTD
			FROM step1 AS t1 
			LEFT JOIN step2 AS t2 
			ON t1.INSURED_ID = t2.INSURED_ID 
			AND t1.ACCOUNTING_PERIOD = t2.ACCOUNTING_PERIOD
		  )

		-------------------------------INSERT TO TMP----------------------------------
		SELECT *
		INTO #etl5_paylife_fyp_driver_detail
		FROM
		  (
		    SELECT
		      CONCAT(YEAR(ACCOUNTING_PERIOD), FORMAT(MONTH(ACCOUNTING_PERIOD), '000')) AS [ACCOUNTING_PERIOD],
		      [TRANSACTION_DATE], 
			  [POL_NO], 
			  [BENF_CD], 
			  [PROD_CD], 
			  [ADJ_TO], 
			  [DIST_CHAN], 
			  [GROSS_PREMIUM], 
			  [GROSS_PREMIUM_YTD], 
			  [APE], 
			  [PCNT_GROSS], 
			  [RENEWAL_PREMIUM], 
			  [PER_SUM_ASSURED], 
			  [NOP_NB], 
			  [PER_ACCOUNT_VALUE], 
			  [COLLECTED_PREMIUM],
			  [COLLECTED_PREMIUM] AS [FYP],
			  [DL_PLAI_BATCHID], 
			  [DL_PLAI_BATCH_RUN_ID], 
			  [DL_PLAI_JOBID], 
			  [DL_PLAI_JOB_RUN_ID], 
			  [DL_PLAI_BATCHDATE], 
			  [ETL_PROCESS_DATE_TIME] 
		    FROM step3 
          ) a;

		---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.[FOND_PAYLIFE_DRIVER_DETAIL] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

		DELETE FROM  [FOND_ID].[FOND_ETL5_PAYLIFE_DRIVER_DETAIL]
					WHERE left(convert(varchar,cast([TRANSACTION_DATE] as date), 112),6) = left(convert(varchar,cast(@trx_date as date), 112),6);
					
		DELETE FROM  [FOND_ID].[FOND_ETL5_PAYLIFE_FYP_DRIVER_DETAIL]
					WHERE left(convert(varchar,cast([TRANSACTION_DATE] as date), 112),6) = left(convert(varchar,cast(@trx_date as date), 112),6); 
		
		
		---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_PAYLIFE_DRIVER_DETAIL] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
		
		INSERT INTO [FOND_ID].[FOND_ETL5_PAYLIFE_DRIVER_DETAIL]
				SELECT * FROM #etl5_paylife_driver_detail; 

		INSERT INTO [FOND_ID].[FOND_ETL5_PAYLIFE_FYP_DRIVER_DETAIL]
				SELECT * FROM #etl5_paylife_fyp_driver_detail; 

		IF @@TRANCOUNT > 0
        COMMIT;

		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#etl5_paylife_driver_detail') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_paylife_driver_detail
		END;

		IF OBJECT_ID('tempdb..#etl5_paylife_fyp_driver_detail') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_paylife_fyp_driver_detail
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
