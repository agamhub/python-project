CREATE PROC [FOND_ID].[USP_LOAD_ETL5_COLLECTED_PREMIUM_DRIVER_DETAIL_YTD] @batch [NVARCHAR](100) AS
BEGIN 
	--DECLARE @batch [nvarchar](30);
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.USP_LOAD_ETL5_COLLECTED_PREMIUM_DRIVER_DETAIL';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @drivername NVARCHAR(15);
	SET @drivername = 'COLLECTEDPREM';
 

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
		IF OBJECT_ID('tempdb..#ETL5_COLLECTED_PREMIUM_DRIVER') IS NOT NULL
		BEGIN
			DROP TABLE #ETL5_COLLECTED_PREMIUM_DRIVER
		END;

		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE ETL5_COLLECTED_PREMIUM_DRIVER : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);		 


		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
		SELECT * 
		INTO #ETL5_COLLECTED_PREMIUM_DRIVER
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
				SUM(CAST(PREMIUM_COLLECTED AS NUMERIC(18,3))) DRIVER_AMOUNT,
				'0' BATCH_MASTER_ID,
				'0' BATCH_RUN_ID,
				'0' JOB_MASTER_ID,
				'0' JOB_RUN_ID,
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
				SUM(CAST([COLLECTED_PREMIUM] AS NUMERIC(18,3))) DRIVER_AMOUNT,
				'0' BATCH_MASTER_ID,
				'0' BATCH_RUN_ID,
				'0' JOB_MASTER_ID,
				'0' JOB_RUN_ID,
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
				SUM(CAST([COLLECTED_PREMIUM] AS NUMERIC(18,3))) DRIVER_AMOUNT,
				'0' BATCH_MASTER_ID,
				'0' BATCH_RUN_ID,
				'0' JOB_MASTER_ID,
				'0' JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
				-- SELECT *
				from [FOND_ID].[FOND_ETL5_PRUAMAN_DRIVER_DETAIL]
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
				SUM(CAST([COLLECTED_PREMIUM] AS NUMERIC(18,3))) DRIVER_AMOUNT,
				'0' BATCH_MASTER_ID,
				'0' BATCH_RUN_ID,
				'0' JOB_MASTER_ID,
				'0' JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
				-- SELECT *
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
				SUM(CAST([COLLECTED_PREMIUM] AS NUMERIC(18,3))) DRIVER_AMOUNT,
				'0' BATCH_MASTER_ID,
				'0' BATCH_RUN_ID,
				'0' JOB_MASTER_ID,
				'0' JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
			FROM [FOND_ID].[FOND_ETL5_PAYLIFE_DRIVER_DETAIL]
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
				SUM(CAST([COLLECTED_PREMIUM] AS NUMERIC(18,3))) DRIVER_AMOUNT,
				'0' BATCH_MASTER_ID,
				'0' BATCH_RUN_ID,
				'0' JOB_MASTER_ID,
				'0' JOB_RUN_ID,
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
				cast(POL_NO as varchar) COLLATE SQL_Latin1_General_CP1_CI_AS POL_NO,
				max([PRODUCT_CODE]) COLLATE SQL_Latin1_General_CP1_CI_AS as BENF_CD,
				max([PRODUCT_CODE]) COLLATE SQL_Latin1_General_CP1_CI_AS as PROD_CD,
				'' [TREATY_CD],
				'GTNN000' COLLATE SQL_Latin1_General_CP1_CI_AS AS FUND,
				@drivername ALLOCATION_DRIVER,
				sum(case when [AMOUNT] is null then 0 else AMOUNT end) as DRIVER_AMOUNT,
				'0' BATCH_MASTER_ID,
				'0' BATCH_RUN_ID,
				'0' JOB_MASTER_ID,
				'0' JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
		FROM (select * from [FOND_ID].[FOND_ETL5_OMNI_DRIVER_DETAIL]) n
		WHERE  cast(left(REPLACE(convert(varchar,RECEIPT_DATE,112),'-',''),4)+'0'+substring(REPLACE(convert(varchar,RECEIPT_DATE,112),'-',''),5,2) as int) 
		BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		GROUP BY [POL_NO]
		)a
		;
		---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.[FOND_ETL5_COLLECTED_PREMIUM_DRIVER_DETAIL] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	 

		DELETE FROM FOND_ID.FOND_ETL5_COLLECTED_PREMIUM_DRIVER_DETAIL 
		WHERE DRIVER_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)));

		 ---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_COLLECTED_PREMIUM_DRIVER_DETAIL] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);


		INSERT INTO [FOND_ID].[FOND_ETL5_COLLECTED_PREMIUM_DRIVER_DETAIL]
		SELECT ENTITY_ID,
				 DRIVER_SOURCE ,
				CAST (DRIVER_PERIOD AS VARCHAR(7)) DRIVER_PERIOD,
				POL_NO,
				BENF_CD,
				PROD_CD,
				TREATY_CD,
				FUND,
				ALLOCATION_DRIVER,
				case when DRIVER_SOURCE='LifeAsia' then DRIVER_AMOUNT else DRIVER_AMOUNT*-1 end DRIVER_AMOUNT,
				BATCH_MASTER_ID,
				BATCH_RUN_ID,
				JOB_MASTER_ID,
				JOB_RUN_ID,
				CAST (DL_PLAI_BATCHDATE AS VARCHAR(6)) DL_PLAI_BATCHDATE,
				ETL_PROCESS_DATE_TIME 
		FROM #ETL5_COLLECTED_PREMIUM_DRIVER
		;

		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #ETL5_COLLECTED_PREMIUM_DRIVER) ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_COLLECTED_PREMIUM_DRIVER_DETAIL'
		,@drivername,@V_TOTAL_ROWS,'YTD',@V_PERIOD);
		

		
		IF @@TRANCOUNT > 0
        COMMIT;


		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#ETL5_COLLECTED_PREMIUM_DRIVER') IS NOT NULL
		BEGIN
			DROP TABLE #ETL5_COLLECTED_PREMIUM_DRIVER
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

