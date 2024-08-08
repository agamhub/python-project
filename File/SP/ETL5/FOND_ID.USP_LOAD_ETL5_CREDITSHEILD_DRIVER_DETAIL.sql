CREATE PROC [FOND_ID].[USP_LOAD_ETL5_CREDITSHEILD_DRIVER_DETAIL] @batch [nvarchar](30),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 
	DECLARE @batchdate [nvarchar](30)=@batch;
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.USP_LOAD_ETL5_CREDITSHEILD_DRIVER_DETAIL';
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
	DECLARE @date date = cast(@batchdate as date) 
--	DECLARE @trx_date DATE = (SELECT convert(date,Dateadd(day, -( Day(@date) ), @date),23));
	DECLARE @trx_date DATE = (SELECT convert(date,Dateadd(day, 0, @date),23));

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
		IF OBJECT_ID('tempdb..#etl5_creditsheild_driver_detail') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_creditsheild_driver_detail
		END;

		IF OBJECT_ID('tempdb..#etl5_creditsheild_fyp_driver_detail') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_creditsheild_fyp_driver_detail
		END;


		with step1 as (
		SELECT 
			left(LEFT(CONVERT(varchar, UPLOAD_DATE,112),6),4)+'0'+right(LEFT(CONVERT(varchar, UPLOAD_DATE,112),6),2)  AS [ACCOUNTING_PERIOD], 
			convert(varchar,DATEADD(month, ((YEAR(UPLOAD_DATE) - 1900) * 12) + MONTH(UPLOAD_DATE), -1),23) AS [TRANSACTION_DATE], 
			B.POLICY_ID as POL_NO, 
			cast(NAME_OF_PRODUCT as varchar) as [BENF_CD],
			cast(NAME_OF_PRODUCT as varchar) as [PROD_CD], 
			cast('GTNN000' as varchar(20)) as [ADJ_TO], 
			null as [DIST_CHAN], 
			PREMIUM * -1 as [GROSS_PREMIUM],  
			case when APE is null then 0 else APE end as [APE], 
			Cast('1' AS BIGINT)  AS [PCNT_GROSS], 
			Cast('0' AS BIGINT) as [RENEWAL_PREMIUM], 
			TOTAL_SUM_ASSURED as [PER_SUM_ASSURED], 
			CASE 
				WHEN B.POLICY_ID is not null THEN Cast('1' AS BIGINT) 
				ELSE Cast('0' AS BIGINT) 
			END as [NOP_NB], 
			Cast('0' AS BIGINT) as [PER_ACCOUNT_VALUE], 
			PREMIUM * -1 as [COLLECTED_PREMIUM], 
			@BATCH_MASTER_ID AS [DL_PLAI_BATCHID],
			@BATCH_RUN_ID AS [DL_PLAI_BATCH_RUN_ID],
			@JOB_MASTER_ID AS [DL_PLAI_JOBID],
			@JOB_RUN_ID AS [DL_PLAI_JOB_RUN_ID], 
			Cast(@batch AS NVARCHAR(6))  AS [DL_PLAI_BATCHDATE], 
			Getdate() AS [ETL_PROCESS_DATE_TIME], 
			B.POLICY_ID AS POLICY_NO,
			UPLOAD_DATE
			--FROM [POSTGRESIFRS17].[prudb].creditshield.stag_premi 
			FROM STAG_ID.STAG_CREDITSHIELD_STAG_PREMI A
			LEFT JOIN (  SELECT POLICY_ID, PRODCD FROM STAG_ID.STAG_CREDITSHIELD_STAG_ETL1_POLICY_RELATED WHERE STATUS = 'INFORCE') B ON A.NAME_OF_PRODUCT = B.PRODCD
			WHERE  Year(UPLOAD_DATE) = Year(@trx_date)), 
			step2 
			AS (SELECT t.POLICY_NO, 
					t.UPLOAD_DATE, 
					Sum(y.[GROSS_PREMIUM]) [GROSS_PREMIUM_YTD] 
				FROM   step1 t 
					JOIN step1 y 
						ON y.POLICY_NO = t.POLICY_NO 
							AND Datediff(year, y.UPLOAD_DATE, t.UPLOAD_DATE) = 0 
							AND y.UPLOAD_DATE <= t.UPLOAD_DATE 
					JOIN step1 m 
						ON m.POLICY_NO = t.POLICY_NO 
							AND Datediff(month, m.UPLOAD_DATE, t.UPLOAD_DATE) = 0 
							AND m.UPLOAD_DATE <= t.UPLOAD_DATE 
				WHERE  Year(t.UPLOAD_DATE) = Year(@trx_date) 
				GROUP  BY t.POLICY_NO, 
						t.UPLOAD_DATE), 
				step3 
				 AS (SELECT t1.*, 
							t2.[GROSS_PREMIUM_YTD] 
					 FROM   step1 t1 
							LEFT JOIN step2 t2 
								   ON t1.POLICY_NO = t2.POLICY_NO 
									  AND t1.UPLOAD_DATE = t2.UPLOAD_DATE)
		-------------------------------INSERT TO TMP----------------------------------
				SELECT * INTO #etl5_creditsheild_driver_detail from (
				SELECT DISTINCT [ACCOUNTING_PERIOD], 
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
		FROM   step3 
		WHERE  left(REPLACE(convert(varchar,UPLOAD_DATE, 112),'-',''),6) = left(convert(varchar,@trx_date, 112),6)
		) a; 


		with step1 as (
		SELECT 
			left(LEFT(CONVERT(varchar, UPLOAD_DATE,112),6),4)+'0'+right(LEFT(CONVERT(varchar, UPLOAD_DATE,112),6),2)  AS [ACCOUNTING_PERIOD], 
			convert(varchar,DATEADD(month, ((YEAR(UPLOAD_DATE) - 1900) * 12) + MONTH(UPLOAD_DATE), -1),23) AS [TRANSACTION_DATE], 
			B.POLICY_ID as POL_NO, 
			cast(NAME_OF_PRODUCT as varchar) as [BENF_CD],
			cast(NAME_OF_PRODUCT as varchar) as [PROD_CD], 
			cast('GTNN000' as varchar(20)) as [ADJ_TO], 
			null as [DIST_CHAN], 
			PREMIUM * -1 as [GROSS_PREMIUM],  
			case when APE is null then 0 else APE end as [APE], 
			Cast('1' AS BIGINT)  AS [PCNT_GROSS], 
			Cast('0' AS BIGINT) as [RENEWAL_PREMIUM], 
			TOTAL_SUM_ASSURED as [PER_SUM_ASSURED], 
			CASE 
				WHEN B.POLICY_ID is not null THEN Cast('1' AS BIGINT) 
				ELSE Cast('0' AS BIGINT) 
			END as [NOP_NB], 
			Cast('0' AS BIGINT) as [PER_ACCOUNT_VALUE], 
			PREMIUM * -1 as [COLLECTED_PREMIUM], 
			@BATCH_MASTER_ID AS [DL_PLAI_BATCHID],
			@BATCH_RUN_ID AS [DL_PLAI_BATCH_RUN_ID],
			@JOB_MASTER_ID AS [DL_PLAI_JOBID],
			@JOB_RUN_ID AS [DL_PLAI_JOB_RUN_ID],
			Cast(@batch AS NVARCHAR(6))  AS [DL_PLAI_BATCHDATE], 
			Getdate() AS [ETL_PROCESS_DATE_TIME], 
			B.POLICY_ID AS POLICY_NO,
			UPLOAD_DATE
			--FROM [POSTGRESIFRS17].[prudb].creditshield.stag_premi 
			FROM STAG_ID.STAG_CREDITSHIELD_STAG_PREMI A
			LEFT JOIN (  SELECT POLICY_ID, PRODCD FROM STAG_ID.STAG_CREDITSHIELD_STAG_ETL1_POLICY_RELATED WHERE STATUS = 'INFORCE') B ON A.NAME_OF_PRODUCT = B.PRODCD
			WHERE  Year(UPLOAD_DATE) = Year(@trx_date)), 
			step2 
			AS (SELECT t.POLICY_NO, 
					t.UPLOAD_DATE, 
					Sum(y.[GROSS_PREMIUM]) [GROSS_PREMIUM_YTD] 
				FROM   step1 t 
					JOIN step1 y 
						ON y.POLICY_NO = t.POLICY_NO 
							AND Datediff(year, y.UPLOAD_DATE, t.UPLOAD_DATE) = 0 
							AND y.UPLOAD_DATE <= t.UPLOAD_DATE 
					JOIN step1 m 
						ON m.POLICY_NO = t.POLICY_NO 
							AND Datediff(month, m.UPLOAD_DATE, t.UPLOAD_DATE) = 0 
							AND m.UPLOAD_DATE <= t.UPLOAD_DATE 
				WHERE  Year(t.UPLOAD_DATE) = Year(@trx_date) 
				GROUP  BY t.POLICY_NO, 
						t.UPLOAD_DATE), 
				step3 
				 AS (SELECT t1.*, 
							t2.[GROSS_PREMIUM_YTD] 
					 FROM   step1 t1 
							LEFT JOIN step2 t2 
								   ON t1.POLICY_NO = t2.POLICY_NO 
									  AND t1.UPLOAD_DATE = t2.UPLOAD_DATE)
		-------------------------------INSERT TO TMP----------------------------------
				SELECT * INTO #etl5_creditsheild_fyp_driver_detail from (
				SELECT DISTINCT [ACCOUNTING_PERIOD], 
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
						[COLLECTED_PREMIUM] AS FYP,
						[DL_PLAI_BATCHID], 
						[DL_PLAI_BATCH_RUN_ID], 
						[DL_PLAI_JOBID], 
						[DL_PLAI_JOB_RUN_ID], 
						[DL_PLAI_BATCHDATE], 
						[ETL_PROCESS_DATE_TIME] 
		FROM   step3 
		WHERE  left(REPLACE(convert(varchar,UPLOAD_DATE, 112),'-',''),6) = left(convert(varchar,@trx_date, 112),6)
		) a; 
		---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.[FOND_ETL5_CREDITSHIELD_DRIVER_DETAIL] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

		DELETE FROM  [FOND_ID].[FOND_ETL5_CREDITSHIELD_DRIVER_DETAIL]
					WHERE left(REPLACE(convert(varchar,[TRANSACTION_DATE], 112),'-',''),6) = left(REPLACE(convert(varchar,@trx_date, 112),'-',''),6); 

		DELETE FROM  [FOND_ID].[FOND_ETL5_CREDITSHIELD_FYP_DRIVER_DETAIL]
					WHERE left(REPLACE(convert(varchar,[TRANSACTION_DATE], 112),'-',''),6) = left(REPLACE(convert(varchar,@trx_date, 112),'-',''),6); 
		
		---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_CREDITSHIELD_DRIVER_DETAIL] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

		INSERT INTO [FOND_ID].[FOND_ETL5_CREDITSHIELD_DRIVER_DETAIL]
				SELECT * FROM #etl5_creditsheild_driver_detail; 

		INSERT INTO [FOND_ID].[FOND_ETL5_CREDITSHIELD_FYP_DRIVER_DETAIL]
				SELECT * FROM #etl5_creditsheild_fyp_driver_detail; 

		IF @@TRANCOUNT > 0
        COMMIT;
		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#etl5_creditsheild_driver_detail') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_creditsheild_driver_detail
		END;

		IF OBJECT_ID('tempdb..#etl5_creditsheild_fyp_driver_detail') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_creditsheild_fyp_driver_detail
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

