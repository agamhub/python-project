CREATE PROC [FOND_ID].[USP_LOAD_ETL5_MJE] @batch [NVARCHAR](100),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.USP_LOAD_ETL5_MJE';
	DECLARE @V_TABLE1 		NVARCHAR(2000);
	DECLARE @V_TABLE2 		NVARCHAR(2000);
	DECLARE @SCHEMA      NVARCHAR(MAX)
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @drivername NVARCHAR(15);
	SET @drivername = 'MJE';
	DECLARE @V_DRIVER_PERIOD VARCHAR(10); 
	SET @V_DRIVER_PERIOD =SUBSTRING(CAST(@batch AS VARCHAR),1,6);
	
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
		

		SET @V_START_DATE	= convert(date, cast(@batch as varchar));
		PRINT	'Start date :' + convert(varchar,@V_START_DATE,112);
		SET @V_START 	= convert(datetime,getDATE());

		SET @V_DESCRIPTION 	= 'Start ' + @V_FUNCTION_NAME + ' : ' + convert(varchar,@V_START,121);
		PRINT	@V_DESCRIPTION;
		SET @V_SEQNO		= @V_SEQNO + 1;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
		---------------------------- DROP TEMPORARY TABLE ------------------------------
		IF OBJECT_ID('tempdb..#MJE') IS NOT NULL
		BEGIN
			DROP TABLE #MJE
		END;
	
		
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE MJE : ' + @batch + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);		 


		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
--		declare @batch varchar(20)='20200701';
		with 
		stepNewCOAnDBCheck as (
		select a.*,
		case when b.COA_lbl is null then 'New COA' else b.COA_lbl end COA_lbl,
		case when a.T7 in (select distinct T7 from STAG_ID.STAG_CONFIG_ETL5_MJE_MASTER_T7_CONFIG) or a.T7 is null or a.T7=''  then 'Existing DB' else 'New DB' end DB_lbl 
		from FOND_ID.FOND_ETL5_SUNGL_EXTRACT a
		left join (select IFRS4SUN_CD,'Existing COA' COA_lbl from STAG_ID.STAG_CONFIG_ETL5_MJE_BS_PNL_LIST_CONFIG) b on a.ACCT_CD=b.IFRS4SUN_CD
		where a.BATCHDATE=left(@batch,6) and a.ENTITY_ID = 'IAI'
		)
		,
		stepFiltered as 
		(
			select n.* from 
			(
				select *,case 
				when JRNAL_TYPE in (
				select JRNAL_TYPE from STAG_ID.STAG_CONFIG_ETL5_MJE_FILTERED_CONFIG 
				where JRNAL_TYPE is not null and JRNAL_TYPE<>''
				) then  'MJE_JRNAL_TYPE'
				when JRNAL_SRC in (
				select JRNAL_SRC from STAG_ID.STAG_CONFIG_ETL5_MJE_FILTERED_CONFIG 
				where JRNAL_SRC is not null and JRNAL_SRC<>''
				) then 'MJE_JRNAL_SRC'
				else 'Unfiltered'
				end F_LBL 		
				from stepNewCOAnDBCheck
			) n where BATCHDATE=left(@batch,6)
		)
		, stepUnfiltered_PCC_RPH as 
		(
			select *,
			case 
			when MJE_UNFILTERED_CATEGORY ='PREMIUM' and F_LBL != 'Unfiltered' then 'Unfiltered-Premium'
			when MJE_UNFILTERED_CATEGORY ='CLAIMS' and F_LBL != 'Unfiltered' then 'Unfiltered-Claims'
			when MJE_UNFILTERED_CATEGORY ='COMMISSIONS' and F_LBL != 'Unfiltered' then 'Unfiltered-Commission'
			when MJE_UNFILTERED_CATEGORY ='REWARD PH' and F_LBL != 'Unfiltered' then 'Unfiltered-Reward PH'
			when COA_lbl='Existing COA' and DB_lbl='Existing DB' and F_LBL != 'Unfiltered' then 'Filtered'
			when COA_lbl='Existing COA' and DB_lbl='Existing DB' and F_LBL = 'Unfiltered' then 'Unfiltered'
			when COA_lbl='New COA'  then 'Unfiltered-New COA'
			when DB_lbl='New DB'  then 'Unfiltered-New DB'
			else '' end MJE_FLAG
			from (
			select n.*,
			case when n.ACCT_CD='6363010000' then 'REWARD PH' 
			else m.MJE_UNFILTERED_CATEGORY end MJE_UNFILTERED_CATEGORY 
			from stepFiltered n
			left join (
			select distinct IFRS4SUN_CD, MJE_UNFILTERED_CATEGORY 
			from STAG_ID.STAG_CONFIG_ETL5_MJE_UNFILTERED_PREM_CLAIM_COMM_CONFIG
			) m on n.ACCT_CD=m.IFRS4SUN_CD
			) l
		) 
		select * into #MJE from stepUnfiltered_PCC_RPH
	
		DELETE FROM FOND_ID.FOND_ETL5_MJE_FILTERED 
		WHERE BATCHDATE =  left(@batch,6) AND ENTITY_ID = 'IAI'
		DELETE FROM FOND_ID.FOND_ETL5_MJE_UNFILTERED 
		WHERE BATCHDATE =  left(@batch,6) AND ENTITY_ID = 'IAI'

		 ---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_MJE_FILTERED] and FOND_ID.[FOND_ETL5_MJE_UNFILTERED] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
		

		INSERT INTO [FOND_ID].[FOND_ETL5_MJE_FILTERED]
		(
		ENTITY_ID,
		ACCT_CD,
		EXTRACT_PERIOD,
		TXN_DT,
		AMT_LOCAL_CCY,
		CR_DR_FLG,
		JRNAL_NO,
		JRNAL_LINE,
		JRNAL_TYPE,
		JRNAL_SRC,
		TXN_REF,
		TXN_DESC,
		T0,
		T1,
		T2,
		T3,
		T4,
		T5,
		T6,
		T7,
		T8,
		MJE_FLAG,
		BATCH_MASTER_ID,
		BATCH_RUN_ID,
		JOB_MASTER_ID,
		JOB_RUN_ID,
		BATCHDATE,
		ETL_PROCESS_DATE_TIME
		)
		SELECT
		ENTITY_ID,
		ACCT_CD,
		EXTRACT_PERIOD,
		TXN_DT,
		AMT_LOCAL_CCY,
		CR_DR_FLG,
		JRNAL_NO,
		JRNAL_LINE,
		JRNAL_TYPE,
		JRNAL_SRC,
		TXN_REF,
		TXN_DESC,
		T0,
		T1,
		T2,
		T3,
		T4,
		T5,
		T6,
		T7,
		T8,
		MJE_FLAG,
		@BATCH_MASTER_ID AS BATCH_MASTER_ID,
		@BATCH_RUN_ID AS BATCH_RUN_ID,
		@JOB_MASTER_ID AS JOB_MASTER_ID,
		@JOB_RUN_ID AS JOB_RUN_ID,
		CONCAT(SUBSTRING(@batch,1,4), SUBSTRING(@batch,5,2)) AS BATCHDATE,
		CURRENT_TIMESTAMP AS ETL_PROCESS_DATE_TIME
		FROM #MJE where MJE_FLAG like '%Filtered%' and ENTITY_ID = 'IAI'
		;
		
		INSERT INTO [FOND_ID].[FOND_ETL5_MJE_UNFILTERED]
		(
		ENTITY_ID,
		ACCT_CD,
		EXTRACT_PERIOD,
		TXN_DT,
		AMT_LOCAL_CCY,
		CR_DR_FLG,
		JRNAL_NO,
		JRNAL_LINE,
		JRNAL_TYPE,
		JRNAL_SRC,
		TXN_REF,
		TXN_DESC,
		T0,
		T1,
		T2,
		T3,
		T4,
		T5,
		T6,
		T7,
		T8,
		MJE_FLAG,
		BATCH_MASTER_ID,
		BATCH_RUN_ID,
		JOB_MASTER_ID,
		JOB_RUN_ID,
		BATCHDATE,
		ETL_PROCESS_DATE_TIME
		)
		SELECT
		ENTITY_ID,
		ACCT_CD,
		EXTRACT_PERIOD,
		TXN_DT,
		AMT_LOCAL_CCY,
		CR_DR_FLG,
		JRNAL_NO,
		JRNAL_LINE,
		JRNAL_TYPE,
		JRNAL_SRC,
		TXN_REF,
		TXN_DESC,
		T0,
		T1,
		T2,
		T3,
		T4,
		T5,
		T6,
		T7,
		T8,
		MJE_FLAG,
		@BATCH_MASTER_ID AS BATCH_MASTER_ID,
		@BATCH_RUN_ID AS BATCH_RUN_ID,
		@JOB_MASTER_ID AS JOB_MASTER_ID,
		@JOB_RUN_ID AS JOB_RUN_ID,
		CONCAT(SUBSTRING(@batch,1,4), SUBSTRING(@batch,5,2)) AS BATCHDATE,
		CURRENT_TIMESTAMP AS ETL_PROCESS_DATE_TIME
		FROM #MJE where MJE_FLAG like '%Unfiltered%' and ENTITY_ID = 'IAI'
		;
		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS_FILTERED integer = 0;
		DECLARE @V_TOTAL_ROWS_UNFILTERED integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS_FILTERED = (SELECT COUNT(1) as totalrows FROM FOND_ID.FOND_ETL5_MJE_FILTERED WHERE ENTITY_ID = 'IAI') ;
		SET @V_TOTAL_ROWS_UNFILTERED = (SELECT COUNT(1) as totalrows FROM FOND_ID.FOND_ETL5_MJE_UNFILTERED WHERE ENTITY_ID = 'IAI') ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_MJE_FILTERED'
		,@drivername,@V_TOTAL_ROWS_FILTERED,'',@V_PERIOD);
		
		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_MJE_UNFILTERED'
		,@drivername,@V_TOTAL_ROWS_UNFILTERED,'',@V_PERIOD);
	
		IF @@TRANCOUNT > 0
        COMMIT;

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

