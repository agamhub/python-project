CREATE PROC [FOND_ID].[USP_LOAD_ETL5_NO_OF_STAFF_STEP3] @batchdate [varchar](6) AS
BEGIN

---------------------------end (GO TO NO OF STAFF)----------------------------
	----------------------------------------------------------
	
	---------------final table sql server---------------------
--	DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='2019011';
--	DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='2019';
--	DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='11';
--	DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='20191101';

	DECLARE  @V_DRIVER_PERIOD VARCHAR(10) =@batchdate;
	DECLARE  @V_ACCT_PERIOD VARCHAR(10) ='';
	DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='';
	DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='';
	DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='';
	set @V_DRIVER_PERIOD_YEAR = left(cast(@V_DRIVER_PERIOD as varchar(10)),4)
	set @V_DRIVER_PERIOD_MONTH = right(cast(@V_DRIVER_PERIOD as varchar(10)),2)
	set @V_DRIVER_PERIOD_FIRST_MONTH = @V_DRIVER_PERIOD_YEAR+@V_DRIVER_PERIOD_MONTH+'01' ;
	set @V_ACCT_PERIOD=@V_DRIVER_PERIOD_YEAR+'0'+@V_DRIVER_PERIOD_MONTH
	BEGIN TRY

	DELETE ifrs17_dw.FOND_ID.FOND_ETL5_NO_STAF_DRIVER_DETAIL WHERE ACCOUNTING_PERIOD=@V_ACCT_PERIOD;
	--DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='201911'
	declare @sql_insertNOS VARCHAR(8000)='INSERT INTO ifrs17_dw.FOND_ID.FOND_ETL5_NO_STAF_DRIVER_DETAIL '+'(ACCOUNTING_PERIOD, POLICY_NO, PRODUCT_CD, BENEFIT_CD, STATUS, PRODUCT_DESC, FUND_CD, CHANNEL, SHARIA_INDICATOR, DRIVER_SOURCE, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness, BATCH_MASTER_ID, BATCH_RUN_ID, JOB_MASTER_ID, JOB_RUN_ID, BATCHDATE, ETL_PROCESS_DATE_TIME)' 
	declare @sql_selectNOS VARCHAR(8000)=
	'
	select '+@V_ACCT_PERIOD+' ACCOUNTING_PERIOD
		  ,CAST([policy_no] AS NVARCHAR(36)) AS POLICY_NO
	      ,CAST([product_cd] AS NVARCHAR(30)) AS PRODUCT_CD
	      ,CAST([benefit_cd] AS NVARCHAR(30)) AS BENEFIT_CD
	      ,CAST([status] AS NVARCHAR(30)) AS STATUS
	      ,CAST([product_desc] AS NVARCHAR(255)) AS PRODUCT_DESC
	      ,CAST([fund_cd] AS NVARCHAR(15)) AS FUND_CD
	      ,CAST([channel] AS NVARCHAR(20)) AS CHANNEL
	      ,CAST([sharia_indicator] AS NVARCHAR(20)) AS SHARIA_INDICATOR
	      ,CAST([driver_source] AS NVARCHAR(15)) AS DRIVER_SOURCE
	      ,[total_policy_per_product]
	      ,[goc]
	      ,CAST([cp_agency_non_sharia_new_bussiness]  AS NUMERIC(18,3)) AS  cp_agency_non_sharia_new_bussiness
	      ,CAST([cp_agency_sharia_new_bussiness] AS NUMERIC(18,3)) AS  cp_agency_sharia_new_bussiness
	      ,CAST([cp_bancassurance_non_sharia_new_bussiness] AS NUMERIC(18,3)) AS  cp_bancassurance_non_sharia_new_bussiness
	      ,CAST([cp_bancassurance_sharia_new_bussiness] AS NUMERIC(18,3)) AS  cp_bancassurance_sharia_new_bussiness
	      ,CAST([cp_dmtm_non_sharia_new_bussiness] AS NUMERIC(18,3)) AS  cp_dmtm_non_sharia_new_bussiness
	      ,CAST([cp_dmtm_sharia_new_bussiness] AS NUMERIC(18,3)) AS  cp_dmtm_sharia_new_bussiness
	      ,CAST([cp_agency_non_sharia_existing_bussiness] AS NUMERIC(18,3)) AS  cp_agency_non_sharia_existing_bussiness
	      ,CAST([cp_agency_sharia_existing_bussiness] AS NUMERIC(18,3)) AS  cp_agency_sharia_existing_bussiness
	      ,CAST([cp_bancassurance_non_sharia_existing_bussiness] AS NUMERIC(18,3)) AS  cp_bancassurance_non_sharia_existing_bussiness 
	      ,CAST([cp_bancassurance_sharia_existing_bussiness] AS NUMERIC(18,3)) AS  cp_bancassurance_sharia_existing_bussiness
	      ,CAST([cp_dmtm_non_sharia_existing_bussiness] AS NUMERIC(18,3)) AS  cp_dmtm_non_sharia_existing_bussiness
	      ,CAST([cp_dmtm_sharia_existing_bussiness] AS NUMERIC(18,3)) AS cp_dmtm_sharia_existing_bussiness
		  ,0 BATCH_MASTER_ID,0 BATCH_RUN_ID,0 JOB_MASTER_ID,0 JOB_RUN_ID,'+@V_DRIVER_PERIOD+' BATCHDATE,0 ETL_PROCESS_DATE_TIME
	from (
		select  policy_no, product_cd, benefit_cd, status, product_desc, fund_cd, channel, sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness
		from ifrs17_dw.FOND_ID.FOND_ETL5_tTEMP_NO_STAFF_PRODUCT_NB_'+@V_DRIVER_PERIOD+'
		union all 
		select policy_no, product_cd, benefit_cd, status, product_desc, fund_cd, channel, sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness 
		from ifrs17_dw.FOND_ID.FOND_ETL5_tTEMP_NO_STAFF_PRODUCT_EB_'+@V_DRIVER_PERIOD+'
	)a 
	-----Depends on how many departement that will be populated'+'
	WHERE 1=1'
	--WHERE cast(goc as varchar(5)) in '+'('+'''A04'''+','+'''A06'''+','+'''A09'''+','+'''A22'''+')'
	
	 
		
	EXEC (@sql_insertNOS+@sql_selectNOS)

--		DECLARE @V_START DATETIME= GETDATE()
--		DECLARE @V_FUNCTION_NAME VARCHAR(200)='FOND_ID.USP_LOAD_ETL5_NO_OF_STAFF_STEP3'
--		DECLARE @drivername VARCHAR(100)='NO_OF_STAFF'
--		DECLARE @V_TOTAL_ROWS integer = 0;
--		DECLARE @V_PERIOD nvarchar(10);
--		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #ETL5_TEMP_REFERENCE_PER_DEPARTMENT) ;
--		declare @batchdate_new varchar(10)=@batchdate+'01'
--        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batchdate_new))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batchdate_new)))),3))
--
--        
--		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
--		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_NO_STAF_DRIVER_DETAIL'
--		,@drivername,@V_TOTAL_ROWS,'YTD',@V_PERIOD);
	
	------FOR TESTING PURPOSE
	DELETE ifrs17_dw.FOND_ID.FOND_ETL5_NO_STAF_WITHOUT_GOC WHERE ACCOUNTING_PERIOD=@V_ACCT_PERIOD;
	INSERT INTO ifrs17_dw.FOND_ID.FOND_ETL5_NO_STAF_WITHOUT_GOC (ACCOUNTING_PERIOD, POLICY_NO, PRODUCT_CD, BENEFIT_CD, STATUS, PRODUCT_DESC, FUND_CD, CHANNEL, SHARIA_INDICATOR, DRIVER_SOURCE, total_policy_per_product, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness)
	SELECT ACCOUNTING_PERIOD, POLICY_NO, PRODUCT_CD, BENEFIT_CD, STATUS, PRODUCT_DESC, FUND_CD, CHANNEL, SHARIA_INDICATOR, DRIVER_SOURCE, 
	    total_policy_per_product, sum(cp_agency_non_sharia_new_bussiness) cp_agency_non_sharia_new_bussiness, sum(cp_agency_sharia_new_bussiness) cp_agency_sharia_new_bussiness, sum(cp_bancassurance_non_sharia_new_bussiness) cp_bancassurance_non_sharia_new_bussiness, sum(cp_bancassurance_sharia_new_bussiness) cp_bancassurance_sharia_new_bussiness, sum(cp_dmtm_non_sharia_new_bussiness) cp_dmtm_non_sharia_new_bussiness, sum(cp_dmtm_sharia_new_bussiness) cp_dmtm_sharia_new_bussiness, sum(cp_agency_non_sharia_existing_bussiness) cp_agency_non_sharia_existing_bussiness, sum(cp_agency_sharia_existing_bussiness) cp_agency_sharia_existing_bussiness, sum(cp_bancassurance_non_sharia_existing_bussiness) cp_bancassurance_non_sharia_existing_bussiness, sum(cp_bancassurance_sharia_existing_bussiness) cp_bancassurance_sharia_existing_bussiness, sum(cp_dmtm_non_sharia_existing_bussiness) cp_dmtm_non_sharia_existing_bussiness, sum(cp_dmtm_sharia_existing_bussiness) cp_dmtm_sharia_existing_bussiness
	    ,'0' BATCH_MASTER_ID,
		'0' BATCH_RUN_ID,
		'0' JOB_MASTER_ID,
		'0' JOB_RUN_ID,
		cast(@batchdate as varchar(6)) as BATCHDATE,
		GETDATE() ETL_PROCESS_DATE_TIME
	from ifrs17_dw.FOND_ID.FOND_ETL5_NO_STAF_DRIVER_DETAIL
	group by total_policy_per_product,ACCOUNTING_PERIOD, POLICY_NO, PRODUCT_CD, BENEFIT_CD, STATUS, PRODUCT_DESC, FUND_CD, CHANNEL, SHARIA_INDICATOR, DRIVER_SOURCE
	--order by CHANNEL ,SHARIA_INDICATOR ,STATUS, DRIVER_SOURCE
	
	END TRY

	BEGIN CATCH
		select ERROR_MESSAGE()
	END CATCH
end	
