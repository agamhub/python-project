CREATE PROC [FOND_ID].[USP_LOAD_ETL5_NOP_NB_DRIVER_DETAIL_YTD] @batch [NVARCHAR](100) AS
BEGIN 
	--DECLARE @batch [nvarchar](30);
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_ETL5_NOP_NB_DRIVER_DETAIL';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @drivername NVARCHAR(15);
	SET @drivername = 'NOPNB';
 

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
		IF OBJECT_ID('tempdb..#etl5_nop_nb_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_nop_nb_driver
		END;

		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE etl5_nop_nb_driver : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);		 


		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
		SELECT * 
		INTO #etl5_nop_nb_driver
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
		        COUNT(DISTINCT [POLICY_NO]) DRIVER_AMOUNT,
				'0' BATCH_MASTER_ID,
				'0' BATCH_RUN_ID,
				'0' JOB_MASTER_ID,
				'0' JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
				-- select top 10 *
		FROM --[FOND_ID].[FOND_LIFEASIA_DRIVER_DETAIL]
		[FOND_ID].[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_SUM_ASSURED_NB_NOP_AGENT]
		WHERE  ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		and 
		NB_FLAG=1
		group by [POLICY_NO]
		UNION all
		SELECT 
				'IAI' ENTITY_ID,
				'PruEmas' DRIVER_SOURCE,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DRIVER_PERIOD,
				[POL_NO],
				max([BENF_CD]) as BENF_CD,
				max([PROD_CD]) as PROD_CD,
				'' [TREATY_CD],
				max([ADJ_T0]) AS FUND,
		        @drivername ALLOCATION_DRIVER,
		        COUNT(DISTINCT [POL_NO]) DRIVER_AMOUNT,
				'0' BATCH_MASTER_ID,
				'0' BATCH_RUN_ID,
				'0' JOB_MASTER_ID,
				'0' JOB_RUN_ID,
				YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 100 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) DL_PLAI_BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME
			  FROM [FOND_ID].[FOND_ETL5_PRUEMAS_DRIVER_DETAIL]
		WHERE  ACCOUNTING_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
		AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		and NOP_NB=1
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
		        COUNT(DISTINCT [POL_NO]) DRIVER_AMOUNT,
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
		and 
		NOP_NB=1
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
		        COUNT(DISTINCT [POL_NO]) DRIVER_AMOUNT,
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
		and NOP_NB =1
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
        COUNT(DISTINCT [POL_NO]) DRIVER_AMOUNT,
		'0' BATCH_MASTER_ID,
		'0' BATCH_RUN_ID,
		'0' JOB_MASTER_ID,
		'0' JOB_RUN_ID,
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
			and NOP_NB=1
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
		        COUNT(DISTINCT [POL_NO]) DRIVER_AMOUNT,
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
		and NOP_NB =1
		GROUP BY [POL_NO]
		)a
		;
		---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.[FOND_ETL5_NOP_NB_DRIVER_DETAIL] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	 

		DELETE FROM FOND_ID.FOND_ETL5_NOP_NB_DRIVER_DETAIL 
		WHERE DRIVER_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)));

		 ---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_NOP_NB_DRIVER_DETAIL] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);


		INSERT INTO [FOND_ID].[FOND_ETL5_NOP_NB_DRIVER_DETAIL]
		SELECT ENTITY_ID,
				DRIVER_SOURCE,
				CAST (DRIVER_PERIOD AS VARCHAR(7)) DRIVER_PERIOD,
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
				CAST (DL_PLAI_BATCHDATE AS VARCHAR(6)) DL_PLAI_BATCHDATE,
				ETL_PROCESS_DATE_TIME 
		FROM #etl5_nop_nb_driver
		;
		
	
		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #etl5_nop_nb_driver) ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_NOP_NB_DRIVER_DETAIL'
		,@drivername,@V_TOTAL_ROWS,'YTD',@V_PERIOD);
		
		
		
		IF @@TRANCOUNT > 0
        COMMIT;


		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#etl5_nop_nb_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_nop_nb_driver
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

