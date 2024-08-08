CREATE PROC [FOND_ID].[USP_LOAD_ETL5_RI_CAT_LOADTOSTAG] @batch [NVARCHAR](100),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 

	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'STAG_ID.STAG_ETL5_RI_CAT_PRE_ALLOCATED';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @drivername NVARCHAR(15);
	SET @drivername = 'LASPRODPERS';

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
		SET @V_START_DATE	= convert(date, cast(@batch as varchar)); -- valuation extract date
		PRINT	'Start date :' + convert(varchar,@V_START_DATE,112);
		SET @V_START 	= convert(datetime,getDATE());

		SET @V_DESCRIPTION 	= 'Start ' + @V_FUNCTION_NAME + ' : ' + convert(varchar,@V_START,121);
		PRINT	@V_DESCRIPTION;
		SET @V_SEQNO		= @V_SEQNO + 1;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
		---------------------------- DROP TEMPORARY TABLE ------------------------------
		IF OBJECT_ID('tempdb..#get_risktype_map') IS NOT NULL
		BEGIN
			DROP TABLE #get_risktype_map
		END;
		IF OBJECT_ID('tempdb..#get_plaidata') IS NOT NULL
		BEGIN
			DROP TABLE #get_plaidata
		END;
		IF OBJECT_ID('tempdb..#get_reindata') IS NOT NULL
		BEGIN
			DROP TABLE #get_reindata
		END;
		IF OBJECT_ID('tempdb..#add_treaty') IS NOT NULL
		BEGIN
			DROP TABLE #add_treaty
		END;
		IF OBJECT_ID('tempdb..#final') IS NOT NULL
		BEGIN
			DROP TABLE #final
		END;

		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE #add_treaty : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);		 


		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
		
		--=========STEP1
		select * into #get_risktype_map 
		from (
			select * from (
			select distinct
			case when substring(GENAREA,83,4) IN('ADBA', 'ADBR','PACR','XPA1','ZPA1') then 'ADBR'
			when substring(GENAREA,83,4) IN('ADDR', 'RDDS', 'DDGI','ADDS' ) then 'RDDS'
			when substring(GENAREA,83,4) in('DTHA', 'DTHR','DTHG','DTGI','DTHS') then 'DTHR'
			when substring(GENAREA,83,4) in('ETPD', 'TPDA', 'TPDR') then 'TPDR'
			when substring(GENAREA,83,4) IN('ADBQ', 'PACQ') then 'ADBQ'     
			when substring(GENAREA,83,4) IN('ADDQ','RDDQ') then 'RDDQ'     
			when substring(GENAREA,83,4) in('DTHQ') then 'DTHQ'     
			when substring(GENAREA,83,4) in('TPDQ') then 'TPDQ'
			when substring(GENAREA,83,4) in('DTCK','DTCI') then substring(GENAREA,83,4)
			else null end as RISKTYPE,
			left(ITEMITEM,3)+'-'+right(ITEMITEM,4) as PLANCODE
		    from STAG_ID.STAG_LIFEASIA_ITEMPF 
			) a where RISKTYPE not in ('RDDS','RDDQ') and RISKTYPE is not null
		) n
		--=========STEP2
		select * into #get_plaidata from (
		select a.POLICYNUMBER as POLICYNUMBER,left(a.PLANCODE,3) as PRODUCT_CD,a.COVERAGE_ID as BENEFIT_CD,a.POLICYCURRENCY as CCY_CD,a.BENEFITSUMASSURED as SAR,a.BENEFITSUMASSURED as RETENTION,a.PLANCODE,
		d.RISKTYPE,c.CONV_SYAR,'' TREATY_ID,b.lbl from STAG_ID.STAG_RENOVA_RENOVA_PREM_INPUT a
		left join 
		(select *,'renova output' as lbl from  STAG_ID.STAG_RENOVA_RENOVA_PREMIUM where POLICYSTATUS='Inf') b
		on a.POLICYNUMBER=b.POLICYNO and a.CUSTOMERID=b.CUSTOMERID
		left join
		(select SUBSTRING(GENAREA,124,2) CONV_SYAR,ITEMITEM from STAG_ID.STAG_LIFEASIA_ITEMPF where ITEMTABL = 'T5687') c  
		on c.ITEMITEM=a.COVERAGE_ID
		left join #get_risktype_map d 
		on a.PLANCODE=d.PLANCODE
		where lbl is null and a.POLICYSTATUS='IF'
		) a
		--=========STEP3
		select * into #get_reindata from (
		select a.POLICYNO as POLICYNUMBER,left(a.LAC,3) as PRODUCT_CD,a.COVERAGEID as BENEFIT_CD,a.CURRENCY as CCY_CD, a.BENEFITSUMASSURED as SAR,a.BENEFITSUMASSURED-a.SUMCEDED as RETENTION,'' TREATY_ID,c.CONV_SYAR,d.RISKTYPE 
		from STAG_ID.STAG_RENOVA_RENOVA_PREMIUM a
		left join
		(select SUBSTRING(GENAREA,124,2) CONV_SYAR,ITEMITEM from STAG_ID.STAG_LIFEASIA_ITEMPF where ITEMTABL = 'T5687') c 
		on c.ITEMITEM=a.COVERAGEID
		left join #get_risktype_map d 
		on a.LAC=d.PLANCODE
		where a.POLICYSTATUS='Inf'
		) a
		--=========STEP4
		select * into #add_treaty from (
		select POLICYNUMBER,PRODUCT_CD,BENEFIT_CD,CCY_CD,SAR,RETENTION,b.TREATY_ID TREATY_ID,CONV_SYAR,RISKTYPE from (
		select POLICYNUMBER,PRODUCT_CD,BENEFIT_CD,CCY_CD,SAR,RETENTION,TREATY_ID,CONV_SYAR,RISKTYPE from #get_plaidata
		union
		select POLICYNUMBER,PRODUCT_CD,BENEFIT_CD,CCY_CD,SAR,RETENTION,TREATY_ID,CONV_SYAR,RISKTYPE from #get_reindata
		) a
		left join 
		(select distinct TREATY_ID,POLICY_NO from FCORE_ID.FOND_RI_ICG_STORE_UC where PRODUCT_CD=BENEFIT_CD) b 
		on a.POLICYNUMBER=b.POLICY_NO
		) n

		select * into #final from (
		select *
		,@BATCH_MASTER_ID AS BATCH_MASTER_ID
		,@BATCH_RUN_ID AS BATCH_RUN_ID
		,@JOB_MASTER_ID AS JOB_MASTER_ID
		,@JOB_RUN_ID AS JOB_RUN_ID
		,SUBSTRING( CAST(@batch AS VARCHAR),1,6) BATCHDATE
		,GETDATE() ETL_PROCESS_DATE_TIME
		from #add_treaty where RISKTYPE is not null
		) b
		
		---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM STAG_ID.STAG_ETL5_RI_CAT_PRE_ALLOCATED : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	 

	
		DELETE FROM STAG_ID.STAG_ETL5_RI_CAT_PRE_ALLOCATED; 

		 ---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE STAG_ID.STAG_ETL5_RI_CAT_PRE_ALLOCATED : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);


		INSERT INTO STAG_ID.STAG_ETL5_RI_CAT_PRE_ALLOCATED (
		POLICYNUMBER,
		PRODUCT_CD,
		BENEFIT_CD,
		CCY_CD,
		SAR,
		RETENTION,
		TREATY_ID,
		CONV_SYAR,
		RISKTYPE,
		BATCH_MASTER_ID,
		BATCH_RUN_ID,
		JOB_MASTER_ID,
		JOB_RUN_ID,
		BATCHDATE,
		ETL_PROCESS_DATE_TIME
		)
		SELECT 
		POLICYNUMBER,
		PRODUCT_CD,
		BENEFIT_CD,
		CCY_CD,
		cast(SAR as float) as SAR,
		cast(RETENTION as float) as RETENTION,
		TREATY_ID,
		CONV_SYAR,
		RISKTYPE,
		BATCH_MASTER_ID,
		BATCH_RUN_ID,
		JOB_MASTER_ID,
		JOB_RUN_ID,
		BATCHDATE,
		ETL_PROCESS_DATE_TIME	
		FROM #final
		;
	
		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #final) ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'STAG_ID.STAG_ETL5_RI_CAT_PRE_ALLOCATED'
		,@drivername,@V_TOTAL_ROWS,'MTD',@V_PERIOD);
		
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

