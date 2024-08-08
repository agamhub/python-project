CREATE PROC [FOND_ID].[USP_LOAD_ETL5_FYP_DRIVER_DETAIL_YTD] @batch [NVARCHAR](100),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 
	--DECLARE @batch [nvarchar](30);
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.USP_LOAD_ETL5_FYP_YTD';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @drivername NVARCHAR(15);
	SET @drivername = 'FYP';
 

BEGIN TRY

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

		SET @V_START_DATE	= convert(date, cast(@batch as varchar)); -- valuation extract date
		PRINT	'Start date :' + convert(varchar,@V_START_DATE,112);
		SET @V_START 	= convert(datetime,getDATE());

		SET @V_DESCRIPTION 	= 'Start ' + @V_FUNCTION_NAME + ' : ' + convert(varchar,@V_START,121);
		PRINT	@V_DESCRIPTION;
		SET @V_SEQNO		= @V_SEQNO + 1;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
		---------------------------- DROP TEMPORARY TABLE ------------------------------
		IF OBJECT_ID('tempdb..#ETL5_FYP_DRIVER_DETAIL') IS NOT NULL
		BEGIN
			DROP TABLE #ETL5_FYP_DRIVER_DETAIL
		END;
		
		IF OBJECT_ID('tempdb..#etl5_fyp_driver_nonPAA') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_fyp_driver_nonPAA
		END;

		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE ETL5_FYP_DRIVER_DETAIL : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);		 


		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
		SELECT * 
		INTO #ETL5_FYP_DRIVER_DETAIL
		FROM (
		SELECT 
				'IAI' ENTITY_ID,
				'LifeAsia' DRIVER_SOURCE,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DRIVER_PERIOD,
				POLICY_NO as POL_NO,
				max(BENF_CD) as BENF_CD,
				max(PROD_CD) as PROD_CD,
				'' TREATY_CD,
				max(ADJ_T0) AS FUND,
		        @drivername AS ALLOCATION_DRIVER,
--				SUM(CAST(FYP_PREMIUM AS NUMERIC(18,3))) DRIVER_AMOUNT,
				SUM(CASE WHEN FIRST_YEAR_PREMIUM_TYPE IN ('Single Premium Topup','Single Premium') THEN FYP_PREMIUM * 0.1 ELSE FYP_PREMIUM END) DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
				-- select top 10 *
		FROM --[FOND_ID].[FOND_LIFEASIA_DRIVER_DETAIL]
		[FOND_ID].[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PREMIUM_COLLECTED_RENEWAL_PREMIUM]
		WHERE  ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		--and PREMIUM_COLLECTED is not null
		group by [POLICY_NO]
		UNION all
		SELECT 
				'IAI' ENTITY_ID,
				'PruEmas' DRIVER_SOURCE,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DRIVER_PERIOD,
				POL_NO ,
				max([BENF_CD]) as BENF_CD,
				max([PROD_CD]) as PROD_CD,
				'' [TREATY_CD],
				max([ADJ_T0]) AS FUND,
		        @drivername ALLOCATION_DRIVER,
				SUM(CAST([FYP] AS NUMERIC(18,3))) DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
			  FROM [FOND_ID].[FOND_ETL5_PRUEMAS_FYP_DRIVER_DETAIL]
		WHERE  ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		GROUP BY [POL_NO]
		UNION all
		SELECT 
				'IAI' ENTITY_ID,
				'PruAman' DRIVER_SOURCE,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DRIVER_PERIOD,
				[POL_NO],
				max([BENF_CD]) as BENF_CD,
				max([PROD_CD]) as PROD_CD,
				'' [TREATY_CD],
				max([ADJ_T0]) AS FUND,
		        @drivername ALLOCATION_DRIVER,
				SUM(CAST([FYP] AS NUMERIC(18,3))) DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
				-- SELECT *
				from [FOND_ID].[FOND_ETL5_PRUAMAN_FYP_DRIVER_DETAIL]
		WHERE  ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		GROUP BY [POL_NO]
		UNION all
		SELECT 
				'IAI' ENTITY_ID,
				'PruAmanS' DRIVER_SOURCE,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DRIVER_PERIOD,
				POL_NO,
				max([BENF_CD]) as BENF_CD,
				max([PROD_CD]) as PROD_CD,
				'' [TREATY_CD],
				max([ADJ_T0]) AS FUND,
		        @drivername ALLOCATION_DRIVER,
				SUM(CAST([FYP] AS NUMERIC(18,3))) DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
				-- SELECT *
				from [FOND_ID].[FOND_ETL5_PRUAMAN_SHARIA_FYP_DRIVER_DETAIL]
			WHERE  ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
			AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
			GROUP BY [POL_NO]
			UNION ALL
			SELECT 
				'IAI' ENTITY_ID,
				'PayLife' DRIVER_SOURCE,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DRIVER_PERIOD,
				[POL_NO],
				max([BENF_CD]) as BENF_CD,
				max([PROD_CD]) as PROD_CD,
				'' [TREATY_CD],
				max([ADJ_T0]) AS FUND,
		        @drivername ALLOCATION_DRIVER,
				SUM(CAST([FYP] AS NUMERIC(18,3))) DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
			FROM [FOND_ID].[FOND_ETL5_PAYLIFE_FYP_DRIVER_DETAIL]
			WHERE  ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
			AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
			GROUP BY [POL_NO]
		UNION ALL
		SELECT 
				'IAI' ENTITY_ID,
				'CreditShield' DRIVER_SOURCE,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DRIVER_PERIOD,
				[POL_NO],
				max([BENF_CD]) as BENF_CD,
				max([PROD_CD]) as PROD_CD,
				'' [TREATY_CD],
				max([ADJ_T0]) AS FUND,
		        @drivername ALLOCATION_DRIVER,
				SUM(CAST([FYP] AS NUMERIC(18,3))) DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
				-- select *
			  FROM [FOND_ID].[FOND_ETL5_CREDITSHIELD_FYP_DRIVER_DETAIL]
		WHERE  ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		GROUP BY [POL_NO]
		UNION all
		SELECT 
				'IAI' ENTITY_ID,
				'DPAS' DRIVER_SOURCE,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DRIVER_PERIOD,
				POL_NO,
				max([BENF_CD]) as BENF_CD,
				max([PROD_CD]) as PROD_CD,
				'' [TREATY_CD],
				max([ADJ_T0]) AS FUND,
		        'FYP_PAA' AS ALLOCATION_DRIVER,
				SUM(CAST([DRIVER_AMOUNT] AS NUMERIC(18,3))) DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
				-- SELECT *
				from FOND_ID.FOND_ETL5_DPAS_DRIVER_DETAIL
			WHERE  DRIVER_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
			AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
			GROUP BY [POL_NO]
		
		)a
		;

		---------------------------- INSERT INTO TEMPORARY TABLE FYP_nonPAA------------------------------
		SELECT  
				ENTITY_ID,
				DRIVER_SOURCE,
				DRIVER_PERIOD,
				POL_NO,
				BENF_CD,
				PROD_CD,
				TREATY_CD,
				FUND,
				'FYP_nonPAA' AS ALLOCATION_DRIVER,
				DRIVER_AMOUNT,
				BATCH_MASTER_ID,
				BATCH_RUN_ID,
				JOB_MASTER_ID,
				JOB_RUN_ID,
				DL_PLAI_BATCHDATE,
				ETL_PROCESS_DATE_TIME
		INTO #etl5_fyp_driver_nonPAA
		FROM #ETL5_FYP_DRIVER_DETAIL
		WHERE UPPER(DRIVER_SOURCE) NOT IN (SELECT DISTINCT CASE WHEN UPPER(SYSTEM) LIKE 'PRUAMANS%' THEN 'PRUAMANS' ELSE UPPER(SYSTEM) END AS SYSTEM FROM STAG_ID.STAG_CONFIG_IFRS17_PORTFOLIO_MAPPING WHERE MEASUREMENT_MODEL = 'PAA');


		---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.[FOND_ETL5_FYP_DRIVER_DETAIL] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	 

		DELETE FROM [FOND_ID].[FOND_ETL5_FYP_DRIVER_DETAIL] 
		WHERE DRIVER_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		AND ENTITY_ID = 'IAI';

		 ---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_FYP_DRIVER_DETAIL] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);


		INSERT INTO [FOND_ID].[FOND_ETL5_FYP_DRIVER_DETAIL]
		SELECT ENTITY_ID,
				 DRIVER_SOURCE ,
				DRIVER_PERIOD,
				POL_NO,
				BENF_CD,
				PROD_CD,
				TREATY_CD,
				FUND,
				ALLOCATION_DRIVER,
				DRIVER_AMOUNT,
				BATCH_MASTER_ID,
				BATCH_RUN_ID,
				JOB_MASTER_ID,
				JOB_RUN_ID,
				DL_PLAI_BATCHDATE,
				ETL_PROCESS_DATE_TIME 
		FROM #ETL5_FYP_DRIVER_DETAIL
		UNION ALL
		SELECT ENTITY_ID,
				 DRIVER_SOURCE ,
				DRIVER_PERIOD,
				POL_NO,
				BENF_CD,
				PROD_CD,
				TREATY_CD,
				FUND,
				ALLOCATION_DRIVER,
				DRIVER_AMOUNT,
				BATCH_MASTER_ID,
				BATCH_RUN_ID,
				JOB_MASTER_ID,
				JOB_RUN_ID,
				DL_PLAI_BATCHDATE,
				ETL_PROCESS_DATE_TIME 
		FROM #etl5_fyp_driver_nonPAA
		

		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #ETL5_FYP_DRIVER_DETAIL) ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.[FOND_ETL5_FYP_DRIVER_DETAIL]'
		,@drivername,@V_TOTAL_ROWS,'YTD',@V_PERIOD);
		

		
		IF @@TRANCOUNT > 0
        COMMIT;


		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#ETL5_FYP_DRIVER_DETAIL') IS NOT NULL
		BEGIN
			DROP TABLE #ETL5_FYP_DRIVER_DETAIL
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
