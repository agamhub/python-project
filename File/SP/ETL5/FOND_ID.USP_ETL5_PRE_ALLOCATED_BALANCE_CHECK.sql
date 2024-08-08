CREATE PROC [FOND_ID].[USP_ETL5_PRE_ALLOCATED_BALANCE_CHECK] @batch [NVARCHAR](100),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 

	DECLARE @V_START		    datetime;
	DECLARE @V_END			    datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'USP_ETL5_PRE_ALLOCATED_BALANCE_CHECK';
	DECLARE @V_DESCRIPTION	    NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_START_DATE	    date;
	DECLARE @V_END_DATE		    date;
	DECLARE @PERIOD             varchar(6) = SUBSTRING(CAST(@batch AS VARCHAR),1,6);
	DECLARE @PERIOD_DT          date = CAST(@batch AS DATE);

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
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DROP TEMPORARY TABLES : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

		IF OBJECT_ID('tempdb..#IBNR') IS NOT NULL BEGIN DROP TABLE #IBNR END;
		IF OBJECT_ID('tempdb..#RI_CAT') IS NOT NULL BEGIN DROP TABLE #RI_CAT END;
		IF OBJECT_ID('tempdb..#SUNGLEXTRACTETL5_IBNR') IS NOT NULL BEGIN DROP TABLE #SUNGLEXTRACTETL5_IBNR END;
		IF OBJECT_ID('tempdb..#SUNGLEXTRACTETL5_RICAT') IS NOT NULL BEGIN DROP TABLE #SUNGLEXTRACTETL5_RICAT END;
		IF OBJECT_ID('tempdb..#SUNGLIFRS4_IBNR') IS NOT NULL BEGIN DROP TABLE #SUNGLIFRS4_IBNR END;
		IF OBJECT_ID('tempdb..#SUNGLIFRS4_RICAT') IS NOT NULL BEGIN DROP TABLE #SUNGLIFRS4_RICAT END;
		IF OBJECT_ID('tempdb..#IBNR_ICG_CHECK') IS NOT NULL BEGIN DROP TABLE #IBNR_ICG_CHECK END;
		IF OBJECT_ID('tempdb..#RICAT_ICG_CHECK') IS NOT NULL BEGIN DROP TABLE #RICAT_ICG_CHECK END;
		IF OBJECT_ID('tempdb..#checking') IS NOT NULL BEGIN DROP TABLE #checking END;
		IF OBJECT_ID('tempdb..#checking2') IS NOT NULL BEGIN DROP TABLE #checking2 END;

		---------------------------- CREATE TEMPORARY TABLE ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'CREATE TEMPORARY TABLES : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

		--TEMP TABLE #IBNR
        select
          ACCT_PERIOD,
          ACCT_CD,
          TXN_TYPE,
          FUND_CD,
          sum(AMT_RPT_CCY) as AMT_RPT_CCY,
          BATCHDATE
        into #IBNR
        from FOND_ID.FOND_PRE_ALLOC_PASS_THROUGH_EXTRACT
        where TXN_TYPE like '%IBNR%' and BATCHDATE = @PERIOD
        group by ACCT_PERIOD,ACCT_CD,TXN_TYPE,FUND_CD,BATCHDATE;
        
		--TEMP TABLE #RI_CAT
        select z.*
        into #RI_CAT
        from
          (
         	select
        	  ACCT_PERIOD,
        	  ACCT_CD,
        	  TXN_TYPE,
        	  FUND_CD,
        	  sum(AMT_RPT_CCY) as AMT_RPT_CCY,
        	  BATCHDATE 
         	from FOND_ID.FOND_PRE_ALLOC_PASS_THROUGH_EXTRACT
         	where TXN_TYPE not like '%IBNR%' and ACCT_CD like '3%' and BATCHDATE = @PERIOD
         	group by ACCT_PERIOD,ACCT_CD,TXN_TYPE,FUND_CD,BATCHDATE
         	
        	union 
         	
        	select
        	  ACCT_PERIOD,
        	  ACCT_CD,
        	  'REVERSE' as TXN_TYPE,
        	  null as FUND_CD,
        	  sum(AMT_RPT_CCY) as AMT_RPT_CCY,
        	  BATCHDATE 
         	from FOND_ID.FOND_PRE_ALLOC_PASS_THROUGH_EXTRACT
         	where TXN_TYPE not like '%IBNR%' and ACCT_CD like '4%' and BATCHDATE = @PERIOD
         	group by ACCT_PERIOD,ACCT_CD,BATCHDATE
          ) as z;
        
		--TEMP TABLE #SUNGLEXTRACTETL5_IBNR
        select
          EXTRACT_PERIOD,
          ACCT_CD,
          sum(cast(AMT_LOCAL_CCY as numeric(28,6))) as AMT_RPT_CCY_ETL5,
          BATCHDATE
        into #SUNGLEXTRACTETL5_IBNR
        from FOND_ID.FOND_ETL5_SUNGL_EXTRACT
        where ACCT_CD in ('5139040000','3141904000') and BATCHDATE = @PERIOD
		AND JRNAL_TYPE like 'ADJ%'
        group by EXTRACT_PERIOD,ACCT_CD,BATCHDATE;
        
		--TEMP TABLE #SUNGLEXTRACTETL5_RICAT
        select
          EXTRACT_PERIOD,
          ACCT_CD,
          sum(cast(AMT_LOCAL_CCY as numeric(28,6))) as AMT_RPT_CCY_ETL5,
          BATCHDATE
        into #SUNGLEXTRACTETL5_RICAT
        from FOND_ID.FOND_ETL5_SUNGL_EXTRACT
        where JRNAL_TYPE like 'ADJ%'  and upper(TXN_DESC) like '%CATASTROPHE%' and BATCHDATE = @PERIOD
        group by EXTRACT_PERIOD,ACCT_CD,BATCHDATE;
        
		--TEMP TABLE #SUNGLIFRS4_IBNR
        select
          PERIOD,
          ACCNT_CODE,
          ANAL_T0,
          ANAL_T7,
          sum(cast(AMOUNT as numeric(28,6))) as AMT_RPT_CCY,
          BATCHDATE
        into #SUNGLIFRS4_IBNR
        from FOND_ID.FOND_ETL5_IFRS4_SUNGL
        where JRNAL_TYPE like 'ADJ%' and ACCNT_CODE in ('5139040000','3141904000') and BATCHDATE = @PERIOD
        group by PERIOD,ACCNT_CODE,ANAL_T0,ANAL_T7,BATCHDATE;
        
		--TEMP TABLE #SUNGLIFRS4_RICAT
        select *
        into #SUNGLIFRS4_RICAT
        from
          (
        	select
        	  SUNGL.PERIOD,
        	  SUNGL.ACCNT_CODE,
        	  sum(cast(SUNGL.AMOUNT as numeric(28,6))) as AMT_RPT_CCY,
        	  SUNGL.BATCHDATE,
        	  SUNGL.TXN_TYPE
        	from
        	  (
        	    select *,
        		  case when ACCNT_CODE='3322500001' and ANAL_T7='TBR'  then 'WEIGHTED_PREM_REINDO_SHAR'
        	           when ACCNT_CODE='3322500001' and ANAL_T7='P01'  then 'WEIGHTED_PREM_REINDO_CONV'
        	           when ACCNT_CODE='3322700001' and ANAL_T7='TBR'  then 'WEIGHTED_PREM_RGA_SHAR'
        	           when ACCNT_CODE='3322700001' and ANAL_T7='P01'  then 'WEIGHTED_PREM_RGA_CONV'
        	           when ACCNT_CODE='3321900001' and ANAL_T7='P01'  then 'WEIGHTED_PREM_MAREIN_CONV'
        	           else 'REVERSE'
        		  end as TXN_TYPE
        	    from FOND_ID.FOND_ETL5_IFRS4_SUNGL
        		where JRNAL_TYPE like 'ADJ%' and upper(DESCRIPTN) like '%CATASTROPHE%' and BATCHDATE = @PERIOD
        	  ) as SUNGL
        	group by SUNGL.PERIOD, SUNGL.ACCNT_CODE, SUNGL.BATCHDATE, SUNGL.TXN_TYPE
          ) as z;
          
		--TEMP TABLE #IBNR_ICG_CHECK
        select *
        into #IBNR_ICG_CHECK
        from
          ( 
        	select
        	  sq_checking.ACCT_PERIOD,
        	  sq_checking.ACCT_CD,
        	  sq_checking.TXN_TYPE,
        	  sq_checking.FUND_CD,
        	  sq_checking.BATCHDATE,
        	  sq_checking.ICG_ID,
        	  sq_checking.TREATY_CHECKING,
        	  sq_checking.ICG_CHECKING
        	from
        	  (
        	    select
        		  pre_alloc_extract.ACCT_PERIOD,
        		  pre_alloc_extract.ACCT_CD,
        		  pre_alloc_extract.TXN_TYPE,
        		  pre_alloc_extract.FUND_CD,
        		  pre_alloc_extract.BATCHDATE,
        		  pre_alloc_extract.ICG_ID, 
        	      case when pre_alloc_extract.TREATY_ID is not null then 'failed' else 'pass' end as TREATY_CHECKING,
        	      case when benefit_icg.INSURANCE_CONTRACT_GROUP_ID  is null then 'failed' else 'pass' end as ICG_CHECKING
        	    from (select * from FOND_ID.FOND_PRE_ALLOC_PASS_THROUGH_EXTRACT where TXN_TYPE like '%IBNR%' and BATCHDATE = @PERIOD) as pre_alloc_extract
        	    left join (
					select INSURANCE_CONTRACT_GROUP_ID AS INSURANCE_CONTRACT_GROUP_ID_ORI, CONCAT(ENTITY_ID,SUBSTRING(INSURANCE_CONTRACT_GROUP_ID,CHARINDEX('_', INSURANCE_CONTRACT_GROUP_ID), LEN(INSURANCE_CONTRACT_GROUP_ID))) AS INSURANCE_CONTRACT_GROUP_ID from FCORE_ID.FOND_BENEFIT_INSURANCE_CONTRACT_GROUP_MASTER where @PERIOD_DT between BATCH_FROM_DT and BATCH_TO_DT group by INSURANCE_CONTRACT_GROUP_ID, CONCAT(ENTITY_ID,SUBSTRING(INSURANCE_CONTRACT_GROUP_ID,CHARINDEX('_', INSURANCE_CONTRACT_GROUP_ID), LEN(INSURANCE_CONTRACT_GROUP_ID)))
					UNION ALL 
					select INSURANCE_CONTRACT_GROUP_ID AS INSURANCE_CONTRACT_GROUP_ID_ORI, CONCAT(ENTITY_ID,SUBSTRING(INSURANCE_CONTRACT_GROUP_ID,CHARINDEX('_', INSURANCE_CONTRACT_GROUP_ID), LEN(INSURANCE_CONTRACT_GROUP_ID))) AS INSURANCE_CONTRACT_GROUP_ID from FCORE_IDIAC.FOND_BENEFIT_INSURANCE_CONTRACT_GROUP_MASTER where @PERIOD_DT between BATCH_FROM_DT and BATCH_TO_DT group by INSURANCE_CONTRACT_GROUP_ID, CONCAT(ENTITY_ID,SUBSTRING(INSURANCE_CONTRACT_GROUP_ID,CHARINDEX('_', INSURANCE_CONTRACT_GROUP_ID), LEN(INSURANCE_CONTRACT_GROUP_ID)))
					UNION ALL
					select INSURANCE_CONTRACT_GROUP_ID AS INSURANCE_CONTRACT_GROUP_ID_ORI, CONCAT(ENTITY_ID,SUBSTRING(INSURANCE_CONTRACT_GROUP_ID,CHARINDEX('_', INSURANCE_CONTRACT_GROUP_ID), LEN(INSURANCE_CONTRACT_GROUP_ID))) AS INSURANCE_CONTRACT_GROUP_ID from FCORE_IDIAS.FOND_BENEFIT_INSURANCE_CONTRACT_GROUP_MASTER where @PERIOD_DT between BATCH_FROM_DT and BATCH_TO_DT group by INSURANCE_CONTRACT_GROUP_ID, CONCAT(ENTITY_ID,SUBSTRING(INSURANCE_CONTRACT_GROUP_ID,CHARINDEX('_', INSURANCE_CONTRACT_GROUP_ID), LEN(INSURANCE_CONTRACT_GROUP_ID)))
				) as benefit_icg on pre_alloc_extract.ICG_ID = benefit_icg.INSURANCE_CONTRACT_GROUP_ID 
        	  ) as sq_checking
        	group by sq_checking.ACCT_PERIOD, sq_checking.ACCT_CD, sq_checking.TXN_TYPE, sq_checking.FUND_CD, sq_checking.BATCHDATE, sq_checking.ICG_ID, sq_checking.TREATY_CHECKING, sq_checking.ICG_CHECKING
          ) as z;
        
		--TEMP TABLE #RICAT_ICG_CHECK
        select *
        into #RICAT_ICG_CHECK
        from
          ( 
        	select distinct
        	  sq_pre_alloc.ACCT_PERIOD,
        	  sq_pre_alloc.ACCT_CD,
        	  case when sq_pre_alloc.ACCT_CD like '4%' then 'REVERSE' else sq_pre_alloc.TXN_TYPE end as TXN_TYPE,
        	  sq_pre_alloc.FUND_CD,
        	  sq_pre_alloc.BATCHDATE,
        	  sq_pre_alloc.ICG_ID,
        	  sq_pre_alloc.TREATY_CHECKING,
        	  sq_pre_alloc.ICG_CHECKING
        	from
        	  (
        	    select
        		  pre_alloc.ACCT_PERIOD,
        		  pre_alloc.ACCT_CD,
        		  pre_alloc.TXN_TYPE,
        		  pre_alloc.FUND_CD,
        		  pre_alloc.BATCHDATE,
        		  pre_alloc.ICG_ID, 
        	      case when icg_map.TREATY_ID is not null or icgs.TREATY_ID is not null then 'pass' else 'failed' end as TREATY_CHECKING,
        	      case when icg_map.RI_ICG_ID  is null or icgs.RI_ICG_ID  is null then 'failed' else 'pass' end as ICG_CHECKING
        	    from (select * from FOND_ID.FOND_PRE_ALLOC_PASS_THROUGH_EXTRACT where TXN_TYPE not like '%IBNR%' and BATCHDATE = @PERIOD) as pre_alloc
        	    left join (
					select RI_ICG_ID, TREATY_ID from FCORE_ID.FOND_RI_UNDERLYING_ICG_MAP where @PERIOD_DT between BATCH_FROM_DT and BATCH_TO_DT group by RI_ICG_ID, TREATY_ID
					UNION ALL
					select RI_ICG_ID, TREATY_ID from FCORE_IDIAC.FOND_RI_UNDERLYING_ICG_MAP where @PERIOD_DT between BATCH_FROM_DT and BATCH_TO_DT group by RI_ICG_ID, TREATY_ID
					UNION ALL
					select RI_ICG_ID, TREATY_ID from FCORE_IDIAS.FOND_RI_UNDERLYING_ICG_MAP where @PERIOD_DT between BATCH_FROM_DT and BATCH_TO_DT group by RI_ICG_ID, TREATY_ID
				) as icg_map on pre_alloc.ICG_ID = icg_map.RI_ICG_ID and pre_alloc.TREATY_ID = icg_map.TREATY_ID
        	    left join (
					select RI_ICG_ID, TREATY_ID from FCORE_ID.FOND_RI_ICG_STORE_UC where @PERIOD_DT between BATCH_FROM_DT and BATCH_TO_DT group by RI_ICG_ID, TREATY_ID
					UNION ALL
					select RI_ICG_ID, TREATY_ID from FCORE_IDIAC.FOND_RI_ICG_STORE_UC where @PERIOD_DT between BATCH_FROM_DT and BATCH_TO_DT group by RI_ICG_ID, TREATY_ID
					UNION ALL
					select RI_ICG_ID, TREATY_ID from FCORE_IDIAS.FOND_RI_ICG_STORE_UC where @PERIOD_DT between BATCH_FROM_DT and BATCH_TO_DT group by RI_ICG_ID, TREATY_ID
				) as icgs on pre_alloc.ICG_ID = icgs.RI_ICG_ID and         pre_alloc.TREATY_ID = icgs.TREATY_ID
        	  ) sq_pre_alloc
          ) as z;
        
		--TEMP TABLE #checking
        select *
        into #checking
        from
          (
        	SELECT
        	  sq.*,
        	  sum(cast(sq.PRE_ALLOC_AMT as numeric(28,6))) over (PARTITION by sq.ACCT_CD,BATCHDATE) as TOTAL_ETL5PREALLOCOUT
        	FROM
        	  (
        	  	select
        		  sq_SUNGLIFRS4_IBNR.*,
        	  	  (sq_SUNGLIFRS4_IBNR.PRE_ALLOC_AMT - sq_SUNGLIFRS4_IBNR.SUNGL_AMT) as DIFF_AMT_SUNGL,
        	  	  SUNGLEXTRACTETL5_IBNR.AMT_RPT_CCY_ETL5 as TOTAL_ETL5SUNGLOUT,
        		  IBNR_ICG_CHECK.TREATY_CHECKING,
        		  IBNR_ICG_CHECK.ICG_CHECKING  
        	  	from
        		  (
        	  	    select
        		      IBNR.ACCT_PERIOD,
        		  	  IBNR.ACCT_CD,
        		  	  IBNR.TXN_TYPE,
        		  	  IBNR.FUND_CD,
        		  	  IBNR.AMT_RPT_CCY as PRE_ALLOC_AMT,
        		  	  SUNGLIFRS4_IBNR.AMT_RPT_CCY as SUNGL_AMT,
        		  	  IBNR.BATCHDATE
        	  	    from #IBNR as IBNR
        	  	    inner join #SUNGLIFRS4_IBNR as SUNGLIFRS4_IBNR on IBNR.ACCT_CD = SUNGLIFRS4_IBNR.ACCNT_CODE and trim(IBNR.FUND_CD) = trim(SUNGLIFRS4_IBNR.ANAL_T0) and IBNR.BATCHDATE = SUNGLIFRS4_IBNR.BATCHDATE
        		  ) as sq_SUNGLIFRS4_IBNR
        	  	left join #SUNGLEXTRACTETL5_IBNR as SUNGLEXTRACTETL5_IBNR on sq_SUNGLIFRS4_IBNR.ACCT_CD = SUNGLEXTRACTETL5_IBNR.ACCT_CD and sq_SUNGLIFRS4_IBNR.BATCHDATE = SUNGLEXTRACTETL5_IBNR.BATCHDATE
        	  	left join #IBNR_ICG_CHECK as IBNR_ICG_CHECK on sq_SUNGLIFRS4_IBNR.ACCT_CD = IBNR_ICG_CHECK.ACCT_CD and IBNR_ICG_CHECK.BATCHDATE = SUNGLEXTRACTETL5_IBNR.BATCHDATE and sq_SUNGLIFRS4_IBNR.TXN_TYPE =         IBNR_ICG_CHECK.TXN_TYPE
        	  	
        		union
        	  	
        		select
        		  sq_SUNGLIFRS4_RICAT.*,
        	  	  (sq_SUNGLIFRS4_RICAT.PRE_ALLOC_AMT - sq_SUNGLIFRS4_RICAT.SUNGL_AMT) as DIFF_AMT_SUNGL,
        	  	  SUNGLEXTRACTETL5_RICAT.AMT_RPT_CCY_ETL5 as TOTAL_ETL5SUNGLOUT,
        		  RICAT_ICG_CHECK.TREATY_CHECKING,
        		  RICAT_ICG_CHECK.ICG_CHECKING   
        	  	from
        		  (
        	  	    select
        			  RI_CAT.ACCT_PERIOD,
        			  RI_CAT.ACCT_CD,
        			  RI_CAT.TXN_TYPE,
        			  RI_CAT.FUND_CD,
        			  RI_CAT.AMT_RPT_CCY as PRE_ALLOC_AMT,
        			  SUNGLIFRS4_RICAT.AMT_RPT_CCY as SUNGL_AMT,
        			  RI_CAT.BATCHDATE
        	  	    from #RI_CAT as RI_CAT
        	  	    inner join #SUNGLIFRS4_RICAT as SUNGLIFRS4_RICAT on RI_CAT.ACCT_CD = SUNGLIFRS4_RICAT.ACCNT_CODE and RI_CAT.TXN_TYPE = SUNGLIFRS4_RICAT.TXN_TYPE and RI_CAT.BATCHDATE = SUNGLIFRS4_RICAT.BATCHDATE
        		  ) sq_SUNGLIFRS4_RICAT
        	  	left join #SUNGLEXTRACTETL5_RICAT as SUNGLEXTRACTETL5_RICAT on sq_SUNGLIFRS4_RICAT.ACCT_CD = SUNGLEXTRACTETL5_RICAT.ACCT_CD and sq_SUNGLIFRS4_RICAT.BATCHDATE = SUNGLEXTRACTETL5_RICAT.BATCHDATE
        	  	left join #RICAT_ICG_CHECK as RICAT_ICG_CHECK on sq_SUNGLIFRS4_RICAT.ACCT_CD = RICAT_ICG_CHECK.ACCT_CD and RICAT_ICG_CHECK.BATCHDATE = SUNGLEXTRACTETL5_RICAT.BATCHDATE and sq_SUNGLIFRS4_RICAT.TXN_TYPE =         RICAT_ICG_CHECK.TXN_TYPE
        	  ) as sq	
          ) as z;
        
		--TEMP TABLE #checking2
        select *
        into #checking2
        from
          (
          	select
        	  sq_checking.*,
        	  case when sq_checking.DIFF_PERCENTAGE < 2 then 'pass' else 'failed' end as RESS_1
          	from
          	  (
          	    select
        		  *,
          		  (TOTAL_ETL5SUNGLOUT - TOTAL_ETL5PREALLOCOUT) as DIFF_AMT_ETL5OUT,
          	      (abs(DIFF_AMT_SUNGL)/abs(SUNGL_AMT)) * 100 as DIFF_PERCENTAGE
          		from #checking
          	  ) sq_checking
          ) as z;
        
		---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM [FOND_ID].[FOND_ETL5_PRE_ALLOCATED_BALANCE_CHECK] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	 
	
		DELETE FROM [FOND_ID].[FOND_ETL5_PRE_ALLOCATED_BALANCE_CHECK]
		WHERE ACCT_PERIOD =  SUBSTRING(CAST(@batch AS VARCHAR),1,4)+'0'+SUBSTRING(CAST(@batch AS VARCHAR),5,2);

		 ---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE [FOND_ID].[FOND_ETL5_PRE_ALLOCATED_BALANCE_CHECK] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

		insert into [FOND_ID].[FOND_ETL5_PRE_ALLOCATED_BALANCE_CHECK]
		  (
		    TREATY_CHECKING,
        	ICG_CHECKING,
        	ACCT_PERIOD,
        	ACCT_CD,
        	TXN_TYPE,
        	FUND_CD,
        	PRE_ALLOC_AMT,
        	SUNGL_AMT,
            DIFF_AMT_SUNGL,
            DIFF_PERCENTAGE,
        	RESS_1,
        	TOTAL_ETL5PREALLOCOUT,
        	TOTAL_ETL5SUNGLOUT,
            DIFF_AMT_ETL5OUT,
			DIFF_PERCENTAGE_ETL5OUT,
			RESS_2,
			BATCH_MASTER_ID, 
			BATCH_RUN_ID, 
			JOB_MASTER_ID, 
			JOB_RUN_ID, 
			BATCHDATE, 
			ETL_PROCESS_DATE_TIME
		  )

        select
          final_checking.*,
          case when final_checking.DIFF_PERCENTAGE_ETL5OUT < 2 then 'pass' else 'failed' end RESS_2
		  ,@BATCH_MASTER_ID AS BATCH_MASTER_ID
			,@BATCH_RUN_ID AS BATCH_RUN_ID
			,@JOB_MASTER_ID AS JOB_MASTER_ID
			,@JOB_RUN_ID AS JOB_RUN_ID
			,SUBSTRING( CAST(@batch AS VARCHAR),1,6) BATCHDATE
			,GETDATE() ETL_PROCESS_DATE_TIME

        from
          (
            select
              TREATY_CHECKING,
        	  ICG_CHECKING,
        	  ACCT_PERIOD,
        	  ACCT_CD,
        	  TXN_TYPE,
        	  FUND_CD,
        	  PRE_ALLOC_AMT,
        	  SUNGL_AMT,
              DIFF_AMT_SUNGL,
              DIFF_PERCENTAGE,
        	  RESS_1,
        	  TOTAL_ETL5PREALLOCOUT,
        	  TOTAL_ETL5SUNGLOUT,
              DIFF_AMT_ETL5OUT,
              (abs(DIFF_AMT_ETL5OUT)/abs(TOTAL_ETL5SUNGLOUT)) * 100 as DIFF_PERCENTAGE_ETL5OUT  
            from #checking2
          ) as final_checking;

		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #checking2) ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_PRE_ALLOCATED_BALANCE_CHECK','',@V_TOTAL_ROWS,'MTD',@V_PERIOD);

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

