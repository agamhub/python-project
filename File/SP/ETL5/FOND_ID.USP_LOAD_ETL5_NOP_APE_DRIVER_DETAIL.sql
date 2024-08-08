CREATE PROC [FOND_ID].[USP_LOAD_ETL5_NOP_APE_DRIVER_DETAIL] @batch [nvarchar](30) AS

BEGIN 
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_MIX_APE_NOP_DRIVER_DETAIL';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @V_SUM_APE numeric(28,6);
	DECLARE @V_SUM_NOP numeric(28,6);
	DECLARE @V_SUM_ALL numeric(28,6);
	DECLARE @V_PERCENTAGE_APE integer = 0;
	DECLARE @V_PERCENTAGE_NOP integer = 0;;
	
	BEGIN TRY

	--SET ANSI_WARNINGS OFF
	
	SET @V_START_DATE	= convert(date, cast(@batch as varchar(8))); -- valuation extract date
	PRINT	'START DATE :' + convert(varchar,@V_START_DATE,112);
	SET @V_START 	= convert(datetime,getDATE());

	SET @V_DESCRIPTION 	= 'START ' + @V_FUNCTION_NAME + ' : ' + convert(varchar,@V_START,121);
	PRINT	@V_DESCRIPTION;
	SET @V_SEQNO		= @V_SEQNO + 1;

	INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
	VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

	---------------------------- DROP TEMPORARY TABLE ------------------------------
	IF OBJECT_ID('tempdb..#driver_ratio') IS NOT NULL
	BEGIN
		DROP TABLE #driver_ratio
	END;
		
	IF OBJECT_ID('tempdb..#driver_ratio_and_allocation') IS NOT NULL
	BEGIN
		DROP TABLE #driver_ratio_and_allocation
	END;
	
	IF OBJECT_ID('tempdb..#driver_sum_table_mixed_driver') IS NOT NULL
	BEGIN
		DROP TABLE #driver_sum_table_mixed_driver
	END;
	 
	IF OBJECT_ID('tempdb..#driver_detail_mix') IS NOT NULL
	BEGIN
		DROP TABLE #driver_detail_mix
	END; 
	
	---------------------------- DROP TEMPORARY TABLE ------------------------------ 

	SELECT	
		@V_PERCENTAGE_APE = A.PERCENTAGE,
		@V_PERCENTAGE_NOP = B.PERCENTAGE
	FROM 
		(SELECT * FROM STAG_ID.STAG_CONFIG_MIX_DRIVER_DETAIL WHERE SINGLE_DRIVER_CODE='APE' ) A INNER JOIN 
		(SELECT * FROM STAG_ID.STAG_CONFIG_MIX_DRIVER_DETAIL WHERE SINGLE_DRIVER_CODE='NOP' ) B
		ON A.MIX_DRIVER_CODE = B.MIX_DRIVER_CODE AND A.SINGLE_DRIVER_CODE <> B.SINGLE_DRIVER_CODE
	;
	
	--PRINT @V_PERCENTAGE_APE;
	--PRINT @V_PERCENTAGE_NOP;
	
	SELECT * 
	INTO 
		#driver_ratio
	FROM (
		SELECT 
			DRIVER_PERIOD,
			'APE_NOP' +  SUBSTRING(@batch,0,7) AS ALLOCATION_DRIVER,
			POL_NO,
			'APE_NOP' MIX_DRIVER_CODE,
			'APE' SINGLE_DRIVER_CODE,
			DRIVER_AMOUNT,
			(DRIVER_AMOUNT * (@V_PERCENTAGE_APE * 0.01)) AS DRIVER_RATIO
		FROM
			(
				SELECT 
					DRIVER_PERIOD,POL_NO,SUM(DRIVER_AMOUNT) DRIVER_AMOUNT
				FROM 
					[FOND_ID].[FOND_ETL5_APE_DRIVER_DETAIL]
				WHERE 
					DRIVER_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
				GROUP BY 
					DRIVER_PERIOD,POL_NO
			)a
		UNION ALL 
		SELECT 
			DRIVER_PERIOD,
			'APE_NOP' +  SUBSTRING(@batch,0,7) AS ALLOCATION_DRIVER,
			POL_NO,
			'APE_NOP' MIX_DRIVER_CODE,
			'NOP' SINGLE_DRIVER_CODE,
			DRIVER_AMOUNT AS DRIVER_AMOUNT,
			(DRIVER_AMOUNT * (@V_PERCENTAGE_NOP * 0.01)) AS DRIVER_RATIO
		FROM
			(	
				SELECT 
					DRIVER_PERIOD,POL_NO,SUM(DRIVER_AMOUNT) DRIVER_AMOUNT
				FROM 
					[FOND_ID].[FOND_ETL5_NOP_DRIVER_DETAIL]
				WHERE 
					DRIVER_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
				GROUP BY 
					DRIVER_PERIOD,POL_NO
				
			)a
    )a;
	
	
	
	SELECT @V_SUM_APE = SUM(CAST(DRIVER_RATIO AS numeric(28,6))) FROM #driver_ratio WHERE SINGLE_DRIVER_CODE='APE';
	SELECT @V_SUM_NOP =  SUM(CAST(DRIVER_RATIO AS numeric(28,6)))  FROM #driver_ratio WHERE SINGLE_DRIVER_CODE='NOP';
	
	PRINT @V_SUM_APE;
	PRINT @V_SUM_NOP;
		
	---------------------------- INSERT TEMPORARY TABLE DRIVER_RATIO_AND_ALLOCATION2 ------------------------------
	SET @V_DESCRIPTION 	= 'INSERT TEMPORARY TABLE DRIVER_RATIO_AND_ALLOCATION : ' + convert(varchar,@V_START,121);
	PRINT	@V_DESCRIPTION;
	SET @V_SEQNO = @V_SEQNO + 1;
	
	INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
	VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION); 

	SELECT
				 *
	INTO
				 #driver_ratio_and_allocation 
	FROM (
				 SELECT a.*,COALESCE(CAST(DRIVER_RATIO AS numeric(28,6))/ NULLIF(cast(@V_SUM_APE as numeric(28,6)),0),0 ) AS WEIGHT FROM #driver_ratio a WHERE SINGLE_DRIVER_CODE='APE'
				 union
				 SELECT a.*,COALESCE(CAST(DRIVER_RATIO AS numeric(28,6))/ NULLIF(cast(@V_SUM_NOP as numeric(28,6)),0),0 ) AS WEIGHT FROM #driver_ratio a WHERE SINGLE_DRIVER_CODE='NOP'
	)a; 

	
	---------------------------- SUM TABLE MIXED DRIVER PER POLICY - RATIOTOTALPOLICY ------------------------------
	SET @V_DESCRIPTION 	= 'SUM TABLE MIXED DRIVER PER POLICY - RATIOTOTALPOLICY: ' + convert(varchar,@V_START,121);
	PRINT	@V_DESCRIPTION;
	SET @V_SEQNO = @V_SEQNO + 1;
	
	INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
	VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

	SELECT 
		DRIVER_PERIOD,
		ALLOCATION_DRIVER,
		POL_NO,
		MIX_DRIVER_CODE,
		SUM(WEIGHT) POLICY_AMOUNT_SUM
	INTO 
		#driver_sum_table_mixed_driver
	FROM 
		#driver_ratio_and_allocation 
	GROUP BY 
		DRIVER_PERIOD,
		ALLOCATION_DRIVER,
		POL_NO,
		MIX_DRIVER_CODE
	;
	

	 SELECT @V_SUM_ALL = SUM(POLICY_AMOUNT_SUM)	
		FROM
		#driver_sum_table_mixed_driver
	;
	
	
	----------------------------  Driver Master - Regional Mapping ------------------------------
	SET @V_DESCRIPTION 	= ' Driver Master - Regional Mapping : ' + convert(varchar,@V_START,121);
	PRINT	@V_DESCRIPTION;
	SET @V_SEQNO = @V_SEQNO + 1;
	
	INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
	VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
	SELECT 
		a.*,case when @V_SUM_ALL=0 then 0 else (CAST(b.POLICY_AMOUNT_SUM AS numeric(28,6))/CAST(@V_SUM_ALL AS numeric(28,6))) end AS DRIVER_AMOUNT 
	INTO	
		#driver_detail_mix
	FROM (
		SELECT 
			DISTINCT 
			*
		FROM (
			SELECT 
				--'APE_NOP' +  SUBSTRING(@batch,0,7) AS ALLOCATION_DRIVER,
				--'APE_NOP' MIX_DRIVER_CODE,
				'APE_NOP' ALLOCATION_DRIVER,
				DRIVER_PERIOD,
				DRIVER_SOURCE,
				ENTITY_ID,
				POL_NO,			
				BENF_CD,
				PROD_CD,
				FUND
			FROM
				[FOND_ID].[FOND_ETL5_APE_DRIVER_DETAIL]
			WHERE 
				DRIVER_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
			UNION
			SELECT 
				--'APE_NOP' +  SUBSTRING(@batch,0,7) AS ALLOCATION_DRIVER,
				--'APE_NOP' MIX_DRIVER_CODE,
				'APE_NOP' ALLOCATION_DRIVER,
				DRIVER_PERIOD,
				DRIVER_SOURCE,
				ENTITY_ID,
				POL_NO,			
				BENF_CD,
				PROD_CD,
				FUND
			FROM
				[FOND_ID].[FOND_ETL5_NOP_DRIVER_DETAIL]
			WHERE 
				DRIVER_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		)a
	)a inner join #driver_sum_table_mixed_driver b 
	on a.POL_NO = b.POL_NO
	; 
	
	---------------------------- INSERT TABLE DRIVER_RATIO_AND_ALLOCATION ------------------------------
	SET @V_DESCRIPTION 	= 'START INSERT INTO DESTINATION TABLE ' + convert(varchar,@V_START,121);
	PRINT	@V_DESCRIPTION;
	SET @V_SEQNO = @V_SEQNO + 1;
	
	INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
	VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	
	
	BEGIN TRANSACTION;
	
	--------------------------------------------------------------------------------
	DELETE FROM FOND_ID.[FOND_ETL5_DRIVER_RATIO_AND_ALLOCATION]
			WHERE 
			1=1 
			AND [DRIVER_PERIOD] = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
			AND MIX_DRIVER_CODE='APE_NOP';
	
	INSERT INTO FOND_ID.FOND_ETL5_DRIVER_RATIO_AND_ALLOCATION
	SELECT DRIVER_PERIOD,ALLOCATION_DRIVER,POL_NO,MIX_DRIVER_CODE,SINGLE_DRIVER_CODE,DRIVER_AMOUNT,DRIVER_RATIO,WEIGHT FROM #driver_ratio_and_allocation;
		
		
	--------------------------------------------------------------------------------
	DELETE FROM [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_TOTAL_RATIO]
			WHERE [DRIVER_PERIOD] = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
			AND MIX_DRIVER_CODE='APE_NOP';
	
	INSERT INTO [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_TOTAL_RATIO] (DRIVER_PERIOD,ALLOCATION_DRIVER,MIX_DRIVER_CODE,SINGLE_DRIVER_CODE,DRIVER_AMOUNT_SUM) 
--	VALUES (
	select
	YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))),
	'APE_NOP' +  SUBSTRING(@batch,0,7) ,
	'APE_NOP',
	'APE',
	@V_SUM_APE
--	);
	;
	
	INSERT INTO [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_TOTAL_RATIO] (DRIVER_PERIOD,ALLOCATION_DRIVER,MIX_DRIVER_CODE,SINGLE_DRIVER_CODE,DRIVER_AMOUNT_SUM) 
--	VALUES (
	select
	YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))),
	'APE_NOP' +  SUBSTRING(@batch,0,7) ,
	'APE_NOP',
	'NOP',
	@V_SUM_NOP
--	);
	;	
	--------------------------------------------------------------------------------
	DELETE FROM [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_TOTAL_RATIO_POLICY] 
		WHERE 
		[DRIVER_PERIOD] = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		AND MIX_DRIVER_CODE='APE_NOP';
	
	INSERT INTO [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_TOTAL_RATIO_POLICY] 
			SELECT * FROM #driver_sum_table_mixed_driver;
	
	
	
	--------------------------------------------------------------------------------
	DELETE FROM  [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_ALL_RATIO]
			WHERE [DRIVER_PERIOD] = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
			AND MIX_DRIVER_CODE='APE_NOP';
	
	INSERT INTO [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_ALL_RATIO] (DRIVER_PERIOD,ALLOCATION_DRIVER,MIX_DRIVER_CODE,POLICY_AMOUNT_SUM) 
--	VALUES (
	select
	YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))),
	'APE_NOP' +  SUBSTRING(@batch,0,7) ,
	'APE_NOP',
	@V_SUM_ALL
--	);
	;	
 	--------------------------------------------------------------------------------
 	DELETE FROM  [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_MIX]
			WHERE [DRIVER_PERIOD] = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
			AND ALLOCATION_DRIVER='APE_NOP';
		
	INSERT INTO [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_MIX]
			SELECT *,'0' BATCH_MASTER_ID,
				'0' BATCH_RUN_ID,
				'0' JOB_MASTER_ID,
				'0' JOB_RUN_ID,
				left(replace(@batch,'-',''),6) as BATCHDATE,
				GETDATE() as ETL_PROCESS_DATE_TIME FROM #driver_detail_mix; 
			
 	IF @@TRANCOUNT > 0
		COMMIT;
				
	
	---------------------------- DROP TERMPORARY TABLE ------------------------------  
	SET @V_DESCRIPTION 	= 'DROP TEMPORARY TABLE ' + convert(varchar,@V_START,121);
	PRINT	@V_DESCRIPTION;
	SET @V_SEQNO = @V_SEQNO + 1;
	
	INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
	VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
	IF OBJECT_ID('tempdb..#driver_ratio') IS NOT NULL
	BEGIN
		DROP TABLE #driver_ratio
	END;
		
	IF OBJECT_ID('tempdb..#driver_ratio_and_allocation') IS NOT NULL
	BEGIN
		DROP TABLE #driver_ratio_and_allocation
	END;
	
	IF OBJECT_ID('tempdb..#driver_sum_table_mixed_driver') IS NOT NULL
	BEGIN
		DROP TABLE #driver_sum_table_mixed_driver
	END;
 	
	IF OBJECT_ID('tempdb..#driver_detail_mix') IS NOT NULL
	BEGIN
		DROP TABLE #driver_detail_mix
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
		
	INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
	VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
	END CATCH
END;

