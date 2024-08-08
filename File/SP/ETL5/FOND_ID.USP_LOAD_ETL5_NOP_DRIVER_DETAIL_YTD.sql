CREATE PROC [FOND_ID].[USP_LOAD_ETL5_NOP_DRIVER_DETAIL_YTD] @batch [NVARCHAR](100),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 
	--DECLARE @batch [nvarchar](30);
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_ETL5_NOP_DRIVER_DETAIL';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @drivername NVARCHAR(15);
	SET @drivername = 'NOP';

	------ START ABC ------
	DECLARE
    @BATCH_MASTER_ID    VARCHAR(20) = 0,
    @BATCH_RUN_ID       VARCHAR(20) = 0,
    @JOB_MASTER_ID      VARCHAR(20) = 0,
    @JOB_RUN_ID         VARCHAR(20) = 0,
    @GMT_START_DTTM     VARCHAR(20) = GETDATE();
	
	EXEC STAG_ID.USP_GetRunIdReturn
	  @JobName        = @JOBNAMESTR,
	  @BATCH_MASTER_ID = @BATCH_MASTER_ID OUTPUT,
	  @BATCH_RUN_ID    = @BATCH_RUN_ID OUTPUT,
	  @JOB_MASTER_ID   = @JOB_MASTER_ID OUTPUT,
	  @JOB_RUN_ID      = @JOB_RUN_ID OUTPUT,
	  @GMT_START_DTTM  = @GMT_START_DTTM OUTPUT;
	------END GET RUN ID DETAIL FROM ABC--------- 
 

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
		IF OBJECT_ID('tempdb..#etl5_nop_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_nop_driver
		END;
		
		IF OBJECT_ID('tempdb..#etl5_nop_driver_nonPAA') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_nop_driver_nonPAA
		END;

		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE etl5_nop_driver : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);		 


		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
		SELECT * 
		INTO #etl5_nop_driver
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
				count(distinct [POLICY_NO]) DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
				-- select top 10 *
		FROM --[FOND_ID].[FOND_LIFEASIA_DRIVER_DETAIL]
		[FOND_ID].[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_SUM_ASSURED_NB_NOP_AGENT]
		WHERE  ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		group by [POLICY_NO]
		UNION all
		SELECT 
				'IAI' ENTITY_ID,
				CASE WHEN LEFT(UPPER(POL_NO),3) = 'PES' THEN 'PruEmasSya' ELSE 'PruEmas' END as DRIVER_SOURCE, --'PruEmas' DRIVER_SOURCE, *should be same as system portofolio config
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DRIVER_PERIOD,
				[POL_NO],
				max([BENF_CD]) as BENF_CD,
				max([PROD_CD]) as PROD_CD,
				'' [TREATY_CD],
				max([ADJ_T0]) AS FUND,
				@drivername ALLOCATION_DRIVER,
				1 DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
			  FROM [FOND_ID].[FOND_ETL5_PRUEMAS_DRIVER_DETAIL]
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
				count(distinct [POL_NO]) DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
				from [FOND_ID].[FOND_ETL5_PRUAMAN_DRIVER_DETAIL]
		WHERE  ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		GROUP BY [POL_NO]
		UNION all
		SELECT 
				'IAI' ENTITY_ID,
				'PruAmanS' DRIVER_SOURCE,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DRIVER_PERIOD,
				[POL_NO],
				max([BENF_CD]) as BENF_CD,
				max([PROD_CD]) as PROD_CD,
				'' [TREATY_CD],
				max([ADJ_T0]) AS FUND,
				@drivername ALLOCATION_DRIVER,
				count(distinct [POL_NO]) DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
				from [FOND_ID].[FOND_ETL5_PRUAMAN_SHARIA_DRIVER_DETAIL]
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
		count(distinct [POL_NO]) DRIVER_AMOUNT,
		@BATCH_MASTER_ID AS BATCH_MASTER_ID,
		@BATCH_RUN_ID AS BATCH_RUN_ID,
		@JOB_MASTER_ID AS JOB_MASTER_ID,
		@JOB_RUN_ID AS JOB_RUN_ID,
		YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
		GETDATE() ETL_PROCESS_DATE_TIME
		FROM 
		  	(
			SELECT 
				[ACCOUNTING_PERIOD],
				[POL_NO],
				[BENF_CD],
				[PROD_CD],
				[ADJ_T0],
				SUM(PCNT_GROSS) AS PCNT_GROSS
				-- SELECT *
			FROM [FOND_ID].[FOND_ETL5_PAYLIFE_DRIVER_DETAIL]
			WHERE  ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
			GROUP BY 
				[ACCOUNTING_PERIOD],
				[POL_NO],
				[BENF_CD],
				[PROD_CD],
				[ADJ_T0]
		)a GROUP BY [POL_NO]
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
				count(distinct [POL_NO]) DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
				-- select *
			  FROM [FOND_ID].[FOND_ETL5_CREDITSHIELD_DRIVER_DETAIL]
		WHERE  ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		GROUP BY [POL_NO]
		UNION ALL
		SELECT 
				'IAI' ENTITY_ID,
				'Omni' DRIVER_SOURCE,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DRIVER_PERIOD,
				cast(POL_NO as varchar) COLLATE SQL_Latin1_General_CP1_CI_AS  POL_NO,
				max([PRODUCT_CODE]) COLLATE SQL_Latin1_General_CP1_CI_AS as BENF_CD,
				max([PRODUCT_CODE]) COLLATE SQL_Latin1_General_CP1_CI_AS as PROD_CD,
				'' [TREATY_CD],
				'GTNN000'  COLLATE SQL_Latin1_General_CP1_CI_AS AS FUND,
				@drivername COLLATE SQL_Latin1_General_CP1_CI_AS as ALLOCATION_DRIVER,
				count( POL_NO) as DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
		FROM (select * from [FOND_ID].[FOND_ETL5_OMNI_DRIVER_DETAIL]) n
		WHERE  cast(left(REPLACE(convert(varchar,RECEIPT_DATE,112),'-',''),4)+'0'+substring(REPLACE(convert(varchar,RECEIPT_DATE,112),'-',''),5,2) as int) 
		BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		GROUP BY [POL_NO],PRODUCT_CODE
		)a
		UNION ALL 
		SELECT 
				'IAI' ENTITY_ID,
				'LifeAsia' DRIVER_SOURCE,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DRIVER_PERIOD,
				[POLICY_NO] as POL_NO,
				max([BENEFITGROUPID]) as BENF_CD,
				max([LAC]) as PROD_CD,
				MAX(TREATYID) AS [TREATY_CD],
				max([FUND_CODE]) AS FUND,
				'NOP_RI' ALLOCATION_DRIVER,
				count( POLICY_NO) DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
				-- select top 10 *
		FROM --[FOND_ID].[FOND_LIFEASIA_DRIVER_DETAIL]
		[FOND_ID].[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_RENOVA]
		WHERE PROCESSINTERVAL BETWEEN CONCAT(YEAR(@batch),'01') AND LEFT(@batch,6)
		group by [POLICY_NO]

		UNION ALL

		SELECT 
				'IAI' AS ENTITY_ID,
				'OMNI_PAA' AS DRIVER_SOURCE,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) AS DRIVER_PERIOD,
				POL_NO,
				max(BENF_CD) AS BENF_CD,
				max(PROD_CD) AS PROD_CD,
				'' AS TREATY_CD,
				max(ADJ_T0) AS FUND,
				'NOP_PAA' AS ALLOCATION_DRIVER,
				count(DISTINCT POL_NO) AS DRIVER_AMOUNT,
				@BATCH_MASTER_ID AS BATCH_MASTER_ID,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) AS DL_PLAI_BATCHDATE,
				GETDATE() AS ETL_PROCESS_DATE_TIME
		FROM FOND_ID.FOND_ETL5_NOP_PAA_DRIVER_DETAIL
		WHERE ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		group by POL_NO
		;
		
		SELECT  
				ENTITY_ID,
				DRIVER_SOURCE,
				DRIVER_PERIOD,
				POL_NO,
				BENF_CD,
				PROD_CD,
				TREATY_CD,
				FUND,
				'NOP_nonPAA' COLLATE SQL_Latin1_General_CP1_CI_AS AS ALLOCATION_DRIVER,
				DRIVER_AMOUNT,
				BATCH_MASTER_ID,
				BATCH_RUN_ID,
				JOB_MASTER_ID,
				JOB_RUN_ID,
				DL_PLAI_BATCHDATE,
				ETL_PROCESS_DATE_TIME
		INTO #etl5_nop_driver_nonPAA
		FROM #etl5_nop_driver
		WHERE ALLOCATION_DRIVER NOT IN ('NOP_RI','NOP_PAA')
		AND UPPER(DRIVER_SOURCE) NOT IN (SELECT DISTINCT UPPER(SYSTEM) AS SYSTEM FROM STAG_ID.STAG_CONFIG_IFRS17_PORTFOLIO_MAPPING WHERE MEASUREMENT_MODEL = 'PAA');
		
		---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.[FOND_ETL5_NOP_DRIVER_DETAIL] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	 

		DELETE FROM FOND_ID.FOND_ETL5_NOP_DRIVER_DETAIL 
		WHERE DRIVER_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		AND ENTITY_ID = 'IAI';

		 ---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_NOP_DRIVER_DETAIL] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);


		INSERT INTO [FOND_ID].[FOND_ETL5_NOP_DRIVER_DETAIL]
		SELECT * FROM #etl5_nop_driver
		UNION ALL
		SELECT * FROM #etl5_nop_driver_nonPAA
		;
	
		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #etl5_nop_driver) ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_NOP_DRIVER_DETAIL'
		,@drivername,@V_TOTAL_ROWS,'YTD',@V_PERIOD);
		
		SELECT 'Total records : ' + cast(@V_TOTAL_ROWS as varchar(500));

		IF @@TRANCOUNT > 0
        COMMIT;


		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#etl5_nop_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_nop_driver
		END;
		
		IF OBJECT_ID('tempdb..#etl5_nop_driver_nonPAA') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_nop_driver_nonPAA
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

