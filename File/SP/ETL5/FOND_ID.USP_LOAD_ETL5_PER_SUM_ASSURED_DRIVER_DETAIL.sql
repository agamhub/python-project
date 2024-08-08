CREATE PROC [FOND_ID].[USP_LOAD_ETL5_PER_SUM_ASSURED_DRIVER_DETAIL] @batch [NVARCHAR](100),@batchid [NVARCHAR](100),@batchrunid [NVARCHAR](100),@jobid [NVARCHAR](100),@jobrunid [NVARCHAR](100),@batchdate [VARCHAR](30) AS

--DECLARE @batch nvarchar(30)='20190201'
--DECLARE @batchid nvarchar(100)= '1'
--DECLARE @batchrunid nvarchar(100)= '1'
--DECLARE @jobid nvarchar(100)= '1'
--DECLARE @jobrunid nvarchar(100)= '1'
--DECLARE @batchdate nvarchar(30)='20190201'
DECLARE @V_MONTH integer = 0;
DECLARE @V_YEAR integer = 0;
DECLARE @YTD FLOAT;


BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_ETL5_PER_SUM_ASSURED_DRIVER_DETAIL';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	
	
	BEGIN TRY
		SET @V_START_DATE	= convert(date, cast(@batch as varchar(8))); -- valuation extract date
		PRINT	'Start date :' + convert(varchar,@V_START_DATE,112);
		SET @V_START 	= convert(datetime,getDATE());

		SET @V_DESCRIPTION 	= 'Start ' + @V_FUNCTION_NAME + ' : ' + convert(varchar,@V_START,121);
		PRINT	@V_DESCRIPTION;
		SET @V_SEQNO		= @V_SEQNO + 1;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
	SET NOCOUNT ON;
	
select @V_MONTH = 
case 
	when month(@batch) = 1 then 12 
	else month(@batch)-1 end 
;    

select  @V_YEAR =
case 
	when @V_MONTH = 12 then year(@batch) - 1 
	else year(@batch) end ;

---------------------------- DROP TEMPORARY TABLE ------------------------------
IF OBJECT_ID('tempdb..#etl5_per_sum_assured_driver') IS NOT NULL
BEGIN
    DROP TABLE #etl5_per_sum_assured_driver
END;

		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TEMPORARY TABLE etl5_pruaman_sharia_driver : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
	
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
SELECT * 
INTO #etl5_per_sum_assured_driver
FROM (
SELECT 
        'IAI' ENTITY_ID,
        'LifeAsia' DRIVER_SOURCE,
        [ACCOUNTING_PERIOD] DRIVER_PERIOD,
        [POLICY_NO] as POL_NO,
        [BENF_CD],
        [PROD_CD],
		'' as [TREATY_CD],
        [ADJ_T0] AS FUND,
        'SUMASSURED' ALLOCATION_DRIVER,
        [PER_SUM_ASSURED] DRIVER_AMOUNT,
        @batchid DL_PLAI_BATCHID,
		@batchid DL_PLAI_BATCH_RUN_ID,
		@batchrunid DL_PLAI_JOBID,
		@jobrunid DL_PLAI_JOB_RUN_ID,
		@batchdate DL_PLAI_BATCHDATE,
		GETDATE() ETL_PROCESS_DATE_TIME
		-- select top 10 *
FROM --[FOND_ID].[FOND_LIFEASIA_DRIVER_DETAIL]
[FOND_ID].[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_SUM_ASSURED_NB_NOP_AGENT]
WHERE  ACCOUNTING_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
UNION all
SELECT 
        'IAI' ENTITY_ID,
        'PruEmas' DRIVER_SOURCE,
        [ACCOUNTING_PERIOD] DRIVER_PERIOD,
        [POL_NO],
        [BENF_CD],
        [PROD_CD],
		'' as [TREATY_CD],
        [ADJ_T0] AS FUND,
        'SUMASSURED' ALLOCATION_DRIVER,
        [PER_SUM_ASSURED] DRIVER_AMOUNT,
		 @batchid DL_PLAI_BATCHID,
		@batchid DL_PLAI_BATCH_RUN_ID,
		@batchrunid DL_PLAI_JOBID,
		@jobrunid DL_PLAI_JOB_RUN_ID,
		@batchdate DL_PLAI_BATCHDATE,
		GETDATE() ETL_PROCESS_DATE_TIME
		 -- select top 10 *
      FROM [AZUREDWDEV].[FOND_ID].[FOND_ETL5_PRUEMAS_DRIVER_DETAIL]
WHERE  ACCOUNTING_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
UNION all
SELECT 
        'IAI' ENTITY_ID,
        'PruAmanS' DRIVER_SOURCE,
        [ACCOUNTING_PERIOD] DRIVER_PERIOD,
        [POL_NO],
        [BENF_CD],
        [PROD_CD],
		'' as [TREATY_CD],
        [ADJ_T0] AS FUND,
        'SUMASSURED' ALLOCATION_DRIVER,
        [PER_SUM_ASSURED] DRIVER_AMOUNT,
		 @batchid DL_PLAI_BATCHID,
		@batchid DL_PLAI_BATCH_RUN_ID,
		@batchrunid DL_PLAI_JOBID,
		@jobrunid DL_PLAI_JOB_RUN_ID,
		@batchdate DL_PLAI_BATCHDATE,
		GETDATE() ETL_PROCESS_DATE_TIME
		 -- select top 10 *
		 from [AZUREDWDEV].[FOND_ID].[FOND_ETL5_PRUAMAN_SHARIA_DRIVER_DETAIL]
WHERE  ACCOUNTING_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
UNION all
SELECT 
        'IAI' ENTITY_ID,
        'PruAman' DRIVER_SOURCE,
        [ACCOUNTING_PERIOD] DRIVER_PERIOD,
        [POL_NO],
        [BENF_CD],
        [PROD_CD],
		'' as [TREATY_CD],
        [ADJ_T0] AS FUND,
        'SUMASSURED' ALLOCATION_DRIVER,
        [PER_SUM_ASSURED] DRIVER_AMOUNT,
        @batchid DL_PLAI_BATCHID,
		@batchid DL_PLAI_BATCH_RUN_ID,
		@batchrunid DL_PLAI_JOBID,
		@jobrunid DL_PLAI_JOB_RUN_ID,
		@batchdate DL_PLAI_BATCHDATE,
		GETDATE() ETL_PROCESS_DATE_TIME
		-- select top 10 *
		from [AZUREDWDEV].[FOND_ID].[FOND_ETL5_PRUAMAN_DRIVER_DETAIL]
WHERE  ACCOUNTING_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
UNION ALL
SELECT 
        'IAI' ENTITY_ID,
        'PayLife' DRIVER_SOURCE,
        [ACCOUNTING_PERIOD] DRIVER_PERIOD,
        [POL_NO],
        [BENF_CD],
        [PROD_CD],
		'' as [TREATY_CD],
        [ADJ_T0] AS FUND,
        'SUMASSURED' ALLOCATION_DRIVER,
        [PER_SUM_ASSURED] DRIVER_AMOUNT,
        @batchid DL_PLAI_BATCHID,
		@batchid DL_PLAI_BATCH_RUN_ID,
		@batchrunid DL_PLAI_JOBID,
		@jobrunid DL_PLAI_JOB_RUN_ID,
		@batchdate DL_PLAI_BATCHDATE,
		GETDATE() ETL_PROCESS_DATE_TIME
		-- select top 10 *
      FROM [AZUREDWDEV].[FOND_ID].[FOND_ETL5_PAYLIFE_DRIVER_DETAIL]
WHERE  ACCOUNTING_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))
UNION ALL
SELECT 
        'IAI' ENTITY_ID,
        'CreditShield' DRIVER_SOURCE,
        [ACCOUNTING_PERIOD] DRIVER_PERIOD,
        [POL_NO],
        [BENF_CD],
        [PROD_CD],
		'' as [TREATY_CD],
        [ADJ_T0] AS FUND,
        'SUMASSURED' ALLOCATION_DRIVER,
        [PER_SUM_ASSURED] DRIVER_AMOUNT,
        @batchid DL_PLAI_BATCHID,
		@batchid DL_PLAI_BATCH_RUN_ID,
		@batchrunid DL_PLAI_JOBID,
		@jobrunid DL_PLAI_JOB_RUN_ID,
		@batchdate DL_PLAI_BATCHDATE,
		GETDATE() ETL_PROCESS_DATE_TIME
		-- select top 10 *
      FROM [AZUREDWDEV].[FOND_ID].[FOND_ETL5_CREDITSHIELD_DRIVER_DETAIL]
WHERE  ACCOUNTING_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)))

)a
;

		---------------------------- TO Handle rerun process ------------------------------
BEGIN TRANSACTION;
SET @V_SEQNO 	= @V_SEQNO + 1;
SET @V_START 	= convert(datetime,getDATE());
SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.FOND_ETL5_PRUAMAN_SHARIA_DRIVER_DETAIL : ' + convert(varchar,@V_START,121);
PRINT @V_DESCRIPTION;

INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);


DELETE FROM FOND_ID.FOND_PER_SUM_ASSURED_DRIVER_DETAIL 
WHERE DRIVER_PERIOD = YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)));

			---------------------------- TO Handle rerun process ------------------------------
SET @V_SEQNO 	= @V_SEQNO + 1;
SET @V_START 	= convert(datetime,getDATE());
SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.FOND_ETL5_PRUAMAN_SHARIA_DRIVER_DETAIL : ' + convert(varchar,@V_START,121);
PRINT @V_DESCRIPTION;

INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

INSERT INTO [FOND_ID].[FOND_ETL5_PER_SUM_ASSURED_DRIVER_DETAIL]
(
[ENTITY_ID]
      ,[DRIVER_SOURCE]
      ,[DRIVER_PERIOD]
      ,[POL_NO]
      ,[BENF_CD]
      ,[PROD_CD]
      ,[TREATY_CD]
      ,[FUND]
      ,[ALLOCATION_DRIVER]
      ,[DRIVER_AMOUNT]
      ,[ETL_PROCESS_DATE_TIME]
      ,[DL_PLAI_BATCHID]
      ,[DL_PLAI_BATCH_RUN_ID]
      ,[DL_PLAI_JOBID]
      ,[DL_PLAI_JOB_RUN_ID]
      ,[DL_PLAI_BATCHDATE]
)
SELECT [ENTITY_ID]
      ,[DRIVER_SOURCE]
      ,[DRIVER_PERIOD]
      ,[POL_NO]
      ,[BENF_CD]
      ,[PROD_CD]
      ,[TREATY_CD]
      ,[FUND]
      ,[ALLOCATION_DRIVER]
      ,[DRIVER_AMOUNT]
      ,[ETL_PROCESS_DATE_TIME]
      ,[DL_PLAI_BATCHID]
      ,[DL_PLAI_BATCH_RUN_ID]
      ,[DL_PLAI_JOBID]
      ,[DL_PLAI_JOB_RUN_ID]
      ,[DL_PLAI_BATCHDATE] FROM #etl5_per_sum_assured_driver;

      IF @@TRANCOUNT > 0
      COMMIT
      ;

 

---------------------------- DROP TEMPORARY TABLE ------------------------------  
IF OBJECT_ID('tempdb..#etl5_per_sum_assured_driver') IS NOT NULL
BEGIN
    DROP TABLE #etl5_per_sum_assured_driver
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
END

