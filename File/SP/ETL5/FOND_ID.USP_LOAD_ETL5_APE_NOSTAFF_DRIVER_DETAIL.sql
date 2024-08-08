CREATE PROC [FOND_ID].[USP_LOAD_ETL5_APE_NOSTAFF_DRIVER_DETAIL] @batch [nvarchar](30) AS

BEGIN 
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_MIX_APE_NOSTAFF_DRIVER_DETAIL';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @V_SUM_APE numeric(28,6);
	DECLARE @V_SUM_NOSTAFF numeric(28,6);
	DECLARE @V_SUM_ALL numeric(28,6);
	DECLARE @V_PERCENTAGE_APE integer = 0;
	DECLARE @V_PERCENTAGE_NOSTAFF integer = 0;;
	

	BEGIN TRY
	

	SET @V_START_DATE	= convert(date, cast(@batch as varchar(8))); -- valuation extract date
	PRINT	'START DATE :' + convert(varchar,@V_START_DATE,112);
	SET @V_START 	= convert(datetime,getDATE());

	SET @V_DESCRIPTION 	= 'START ' + @V_FUNCTION_NAME + ' : ' + convert(varchar,@V_START,121);
	PRINT	@V_DESCRIPTION;
	SET @V_SEQNO		= @V_SEQNO + 1;



	---------------------------- DROP TEMPORARY TABLE ------------------------------
	IF OBJECT_ID('tempdb..#driver_ratio_ape_nostaff') IS NOT NULL
	BEGIN
		DROP TABLE #driver_ratio_ape_nostaff
	END;
		
	IF OBJECT_ID('tempdb..#driver_ratio_and_allocation_ape_nostaff') IS NOT NULL
	BEGIN
		DROP TABLE #driver_ratio_and_allocation_ape_nostaff
	END;

	IF OBJECT_ID('tempdb..#driver_sum_table_mixed_driver_ape_nostaff') IS NOT NULL
	BEGIN
		DROP TABLE #driver_sum_table_mixed_driver_ape_nostaff
	END;
	 
	IF OBJECT_ID('tempdb..#driver_detail_mix_ape_nostaff') IS NOT NULL
	BEGIN
		DROP TABLE #driver_detail_mix_ape_nostaff
	END; 
	
	---------------------------- DROP TEMPORARY TABLE ------------------------------ 
	INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
	VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
	SELECT	
		@V_PERCENTAGE_APE = A.PERCENTAGE,
		@V_PERCENTAGE_NOSTAFF = B.PERCENTAGE
	FROM 
		(SELECT * FROM STAG_ID.STAG_CONFIG_MIX_DRIVER_DETAIL WHERE SINGLE_DRIVER_CODE='APE' and MIX_DRIVER_CODE='APE_NOSTAFF') A INNER JOIN 
		(SELECT * FROM STAG_ID.STAG_CONFIG_MIX_DRIVER_DETAIL WHERE SINGLE_DRIVER_CODE='NOSTAFF' and MIX_DRIVER_CODE='APE_NOSTAFF') B
		ON A.MIX_DRIVER_CODE = B.MIX_DRIVER_CODE AND A.SINGLE_DRIVER_CODE <> B.SINGLE_DRIVER_CODE
	;
	
	PRINT @V_PERCENTAGE_APE;
	PRINT @V_PERCENTAGE_NOSTAFF;
	

	SELECT * 
	INTO 
		#driver_ratio_ape_nostaff
	FROM (
		SELECT 
			DRIVER_PERIOD COLLATE DATABASE_DEFAULT as DRIVER_PERIOD,
			'APE_NOSTAFF' +  SUBSTRING(@batch,0,7) COLLATE DATABASE_DEFAULT AS ALLOCATION_DRIVER,
			POL_NO COLLATE DATABASE_DEFAULT as POL_NO,
			'APE_NOSTAFF' MIX_DRIVER_CODE,
			'APE' SINGLE_DRIVER_CODE,
			cast(DRIVER_AMOUNT as numeric(28,6)) as DRIVER_AMOUNT,
			cast((DRIVER_AMOUNT * (@V_PERCENTAGE_APE * 0.01)) as numeric(28,6)) AS DRIVER_RATIO
		FROM
			(
				SELECT DRIVER_PERIOD,POL_NO,sum(DRIVER_AMOUNT) as DRIVER_AMOUNT  from [FOND_ID].[FOND_ETL5_APE_DRIVER_DETAIL] 
				WHERE 1=1 
				AND DRIVER_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
				group by POL_NO,DRIVER_PERIOD			
			) t1
		UNION ALL
		SELECT 
			ACCOUNTING_PERIOD COLLATE DATABASE_DEFAULT as DRIVER_PERIOD,
			'APE_NOSTAFF' +  SUBSTRING(@batch,0,7) COLLATE DATABASE_DEFAULT AS ALLOCATION_DRIVER,
			POLICY_NO COLLATE DATABASE_DEFAULT as POL_NO,
			'APE_NOSTAFF' MIX_DRIVER_CODE,
			'NOSTAFF' SINGLE_DRIVER_CODE,
			cast(DRIVER_AMOUNT as numeric(28,6))  as DRIVER_AMOUNT,
			cast((DRIVER_AMOUNT * (@V_PERCENTAGE_NOSTAFF * 0.01)) as numeric(28,6)) AS DRIVER_RATIO
		FROM
			(
				SELECT  ACCOUNTING_PERIOD,
					   POLICY_NO,
					   SUM(COALESCE(cp_agency_non_sharia_new_bussiness,0)) + SUM(COALESCE(cp_agency_sharia_new_bussiness,0))  + SUM(COALESCE(cp_bancassurance_non_sharia_new_bussiness,0)) 
					   + SUM(COALESCE(cp_bancassurance_sharia_new_bussiness,0))  + SUM(COALESCE(cp_dmtm_non_sharia_new_bussiness,0))  + SUM(COALESCE(cp_dmtm_sharia_new_bussiness,0)) 
					   + SUM(COALESCE(cp_agency_non_sharia_existing_bussiness,0))  + SUM(COALESCE(cp_agency_sharia_existing_bussiness,0))  + SUM(COALESCE(cp_bancassurance_non_sharia_existing_bussiness,0)) 
					   + SUM(COALESCE(cp_bancassurance_sharia_existing_bussiness,0))  + SUM(COALESCE(cp_dmtm_non_sharia_existing_bussiness,0))  + SUM(COALESCE(cp_dmtm_sharia_existing_bussiness,0))  
					   as DRIVER_AMOUNT
				FROM FOND_ID.FOND_ETL5_NO_STAF_DRIVER_DETAIL
				WHERE 
					1=1 
					and POLICY_NO IS NOT NULL
					and	ACCOUNTING_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
				GROUP BY POLICY_NO,ACCOUNTING_PERIOD
			) A
    )a;
	

	

	SELECT @V_SUM_APE = SUM(CAST(DRIVER_RATIO AS numeric(28,6)))  FROM #driver_ratio_ape_nostaff WHERE SINGLE_DRIVER_CODE='APE';
	SELECT @V_SUM_NOSTAFF = SUM(CAST(DRIVER_RATIO AS numeric(28,6)))  FROM #driver_ratio_ape_nostaff  WHERE SINGLE_DRIVER_CODE='NOSTAFF';
	
	PRINT  @V_SUM_APE;
	PRINT @V_SUM_NOSTAFF;


	---------------------------- INSERT TEMPORARY TABLE DRIVER_RATIO_AND_ALLOCATION2 ------------------------------
	SET @V_DESCRIPTION 	= 'INSERT TEMPORARY TABLE DRIVER_RATIO_AND_ALLOCATION : ' + convert(varchar,@V_START,121);
	PRINT	@V_DESCRIPTION;
	SET @V_SEQNO = @V_SEQNO + 1;
	
	INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
	VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
 	SELECT
		*
	INTO
		#driver_ratio_and_allocation_ape_nostaff 
	FROM (
		SELECT a.*,COALESCE(CAST(DRIVER_RATIO AS numeric(28,6))/ NULLIF(cast(@V_SUM_APE as numeric(28,6)),0),0 )  AS WEIGHT FROM #driver_ratio_ape_nostaff a WHERE SINGLE_DRIVER_CODE='APE'
		union
		SELECT a.*,COALESCE(CAST(DRIVER_RATIO AS numeric(28,6))/ NULLIF(cast(@V_SUM_NOSTAFF as numeric(28,6)),0),0 ) AS WEIGHT FROM #driver_ratio_ape_nostaff a WHERE SINGLE_DRIVER_CODE='NOSTAFF'
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
		#driver_sum_table_mixed_driver_ape_nostaff 
	FROM 
		#driver_ratio_and_allocation_ape_nostaff  
	GROUP BY 
		DRIVER_PERIOD,
		ALLOCATION_DRIVER,
		POL_NO,
		MIX_DRIVER_CODE
	;


	SELECT 
		@V_SUM_ALL = SUM(POLICY_AMOUNT_SUM)	
	FROM
		#driver_sum_table_mixed_driver_ape_nostaff 
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
		#driver_detail_mix_ape_nostaff
	FROM (
		SELECT 
			DISTINCT 
			*
		FROM (
			SELECT 
				'APE_NOSTAFF' ALLOCATION_DRIVER,
				DRIVER_PERIOD COLLATE DATABASE_DEFAULT as DRIVER_PERIOD,
				DRIVER_SOURCE COLLATE DATABASE_DEFAULT as DRIVER_SOURCE,
				'IAI' as ENTITY_ID,
				POL_NO COLLATE DATABASE_DEFAULT as POL_NO,			
				BENF_CD COLLATE DATABASE_DEFAULT as BENF_CD,
				PROD_CD COLLATE DATABASE_DEFAULT as PROD_CD,
				FUND COLLATE DATABASE_DEFAULT as FUND
			FROM
				[FOND_ID].[FOND_ETL5_APE_DRIVER_DETAIL]
			WHERE 
				1=1 
				AND DRIVER_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
			UNION
			SELECT 
				'APE_NOSTAFF' ALLOCATION_DRIVER,
				ACCOUNTING_PERIOD COLLATE DATABASE_DEFAULT as DRIVER_PERIOD,
				DRIVER_SOURCE COLLATE DATABASE_DEFAULT as DRIVER_SOURCE,
				'IAI' as ENTITY_ID,
				POLICY_NO COLLATE DATABASE_DEFAULT as POL_NO,			
				CASE WHEN upper(DRIVER_SOURCE) like 'CREDITSHI%' then 'CSPLUS' else BENEFIT_CD end COLLATE DATABASE_DEFAULT BENF_CD,
				CASE WHEN upper(DRIVER_SOURCE) like 'CREDITSHI%' then 'CSPLUS' else PRODUCT_CD end COLLATE DATABASE_DEFAULT PROD_CD,
				FUND_CD COLLATE DATABASE_DEFAULT as FUND
	    	FROM
				[FOND_ID].FOND_ETL5_NO_STAF_DRIVER_DETAIL 
			WHERE 
				1=1 
				AND ACCOUNTING_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		)a
	)a inner join #driver_sum_table_mixed_driver_ape_nostaff b 
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
			WHERE [DRIVER_PERIOD] = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) 
			and MIX_DRIVER_CODE='APE_NOSTAFF';
	
	INSERT INTO FOND_ID.FOND_ETL5_DRIVER_RATIO_AND_ALLOCATION
			SELECT * FROM #driver_ratio_and_allocation_ape_nostaff;
		
	--------------------------------------------------------------------------------
	DELETE FROM [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_TOTAL_RATIO_POLICY] 
		WHERE [DRIVER_PERIOD] = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
		and MIX_DRIVER_CODE='APE_NOSTAFF';
	
	INSERT INTO [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_TOTAL_RATIO_POLICY] 
			SELECT * FROM #driver_sum_table_mixed_driver_ape_nostaff;
	
	--------------------------------------------------------------------------------
	DELETE FROM [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_TOTAL_RATIO]
			WHERE [DRIVER_PERIOD] = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) 
			and MIX_DRIVER_CODE='APE_NOSTAFF';
	
	INSERT INTO [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_TOTAL_RATIO] (DRIVER_PERIOD,ALLOCATION_DRIVER,MIX_DRIVER_CODE,SINGLE_DRIVER_CODE,DRIVER_AMOUNT_SUM) 
	-- VALUES (
	select
	YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))),
	'APE_NOSTAFF' +  SUBSTRING(@batch,0,7) ,
	'APE_NOSTAFF',
	'APE',
	@V_SUM_APE
	-- );
	;
	
	INSERT INTO [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_TOTAL_RATIO] (DRIVER_PERIOD,ALLOCATION_DRIVER,MIX_DRIVER_CODE,SINGLE_DRIVER_CODE,DRIVER_AMOUNT_SUM) 
	-- VALUES (
	select
	YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))),
	'APE_NOSTAFF' +  SUBSTRING(@batch,0,7) ,
	'APE_NOSTAFF',
	'NOSTAFF',
	@V_SUM_NOSTAFF
	-- );
	;
	--------------------------------------------------------------------------------
	DELETE FROM  [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_ALL_RATIO]
			WHERE [DRIVER_PERIOD] = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) 
			and MIX_DRIVER_CODE='APE_NOSTAFF';
	
	INSERT INTO [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_ALL_RATIO] (DRIVER_PERIOD,ALLOCATION_DRIVER,MIX_DRIVER_CODE,POLICY_AMOUNT_SUM) 
	-- VALUES (
	select
	YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))),
	'APE_NOSTAFF' +  SUBSTRING(@batch,0,7) ,
	'APE_NOSTAFF',
	@V_SUM_ALL
	-- );
	;
 	--------------------------------------------------------------------------------
 	DELETE FROM  [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_MIX]
			WHERE [DRIVER_PERIOD] = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) 
			and ALLOCATION_DRIVER='APE_NOSTAFF';
		
	INSERT INTO [FOND_ID].[FOND_ETL5_DRIVER_DETAIL_MIX]
			SELECT *,'0' BATCH_MASTER_ID,
				'0' BATCH_RUN_ID,
				'0' JOB_MASTER_ID,
				'0' JOB_RUN_ID,
				left(replace(@batch,'-',''),6) as BATCHDATE,
				GETDATE() as ETL_PROCESS_DATE_TIME FROM #driver_detail_mix_ape_nostaff; 
			
 	IF @@TRANCOUNT > 0
		COMMIT;
				
	
	---------------------------- DROP TERMPORARY TABLE ------------------------------  
	SET @V_DESCRIPTION 	= 'DROP TEMPORARY TABLE ' + convert(varchar,@V_START,121);
	PRINT	@V_DESCRIPTION;
	SET @V_SEQNO = @V_SEQNO + 1; 
	
	INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
	VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
	IF OBJECT_ID('tempdb..#driver_ratio_ape_nostaff') IS NOT NULL
	BEGIN
		DROP TABLE #driver_ratio_ape_nostaff
	END;
		
	IF OBJECT_ID('tempdb..#driver_ratio_and_allocation_ape_nostaff') IS NOT NULL
	BEGIN
		DROP TABLE #driver_ratio_and_allocation_ape_nostaff
	END;

	IF OBJECT_ID('tempdb..#driver_sum_table_mixed_driver_ape_nostaff') IS NOT NULL
	BEGIN
		DROP TABLE #driver_sum_table_mixed_driver_ape_nostaff
	END;
	 
	IF OBJECT_ID('tempdb..#driver_detail_mix_ape_nostaff') IS NOT NULL
	BEGIN
		DROP TABLE #driver_detail_mix_ape_nostaff
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
	raiserror(@V_DESCRIPTION, 18, 1)
	END CATCH
END;


