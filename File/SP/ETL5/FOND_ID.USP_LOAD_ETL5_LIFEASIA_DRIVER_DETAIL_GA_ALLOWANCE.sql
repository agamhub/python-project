CREATE PROC [FOND_ID].[USP_LOAD_ETL5_LIFEASIA_DRIVER_DETAIL_GA_ALLOWANCE] @batch [nvarchar](100),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 
	--DECLARE @batch [nvarchar](30);
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_GA_ALLOWANCE';
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
	SET @drivername = 'LASGAALLOWANCE';

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
		IF OBJECT_ID('tempdb..#etl5_las_ga_allowance_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_las_ape_driver
		END;
		IF OBJECT_ID('tempdb..#ga_allowance') IS NOT NULL
		BEGIN
			DROP TABLE #ga_allowance
		END;
		IF OBJECT_ID('tempdb..#chdr') IS NOT NULL
		BEGIN
			DROP TABLE #chdr
		END;
		IF OBJECT_ID('tempdb..#hpad') IS NOT NULL
		BEGIN
			DROP TABLE #hpad
		END;
		IF OBJECT_ID('tempdb..#covr') IS NOT NULL
		BEGIN
			DROP TABLE #covr
		END;
		IF OBJECT_ID('tempdb..#T0_2') IS NOT NULL
		BEGIN
			DROP TABLE #T0_2
		END;
		IF OBJECT_ID('tempdb..#FINAL') IS NOT NULL
		BEGIN
			DROP TABLE #FINAL
		END;

		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE etl5_las_ape_driver : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);		 


		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
		--DROP TABLE #ga_allowance;
		--declare @batch varchar(12)='20210101'
		SELECT
		        POLICY_ID 
		        ,SUBSTRING(CAST(DATE_FROM AS VARCHAR) ,1,4) YEAR_DATE_TO
		        ,DATE_TO,DATE_FROM ,SUBSTRING (CAST (DATE_TO AS VARCHAR),1,4)+'0'+SUBSTRING (CAST(DATE_TO AS VARCHAR),5,2) year_month,
		        SUM(COALESCE(FYP_FIXED_ALLOWANCE, 0))+SUM(COALESCE(FYP_VARIABLE_ALLOWANCE,0))+SUM(COALESCE(SYP_FIXED_ALLOWANCE,0))+SUM(COALESCE(SYP_VARIABLE_ALLOWANCE,0))+SUM(COALESCE(TYP_PLUS_FIXED_ALLOWANCE,0))+SUM(COALESCE(TYP_PLUS_VARIABLE_ALLOWANCE,0))+SUM(COALESCE(SP_FIXED_ALLOWANCE,0))+SUM(COALESCE(SP_VARIABLE_ALLOWANCE,0)) AS ga_allowance
		    INTO #ga_allowance
		    from STAG_ID.STAG_LIFEASIA_GA_ALLOWANCE 
		    where LEN(POLICY_ID) >= 8
		    and SUBSTRING(CAST(DATE_FROM AS VARCHAR),1,6) = SUBSTRING(CAST(@batch AS VARCHAR),1,6) 
		    group by POLICY_ID,SUBSTRING(CAST(DATE_FROM AS VARCHAR),1,4),DATE_TO,DATE_FROM
		
		;
		
		--drop table IF EXISTS #chdr;
		select a.* 
		into #chdr
		from (
		select row_number () OVER (
			   PARTITION BY CHDRNUM order by TRANNO desc
			) rank,
		a.* from (
		select TRANNO ,CHDRNUM ,CNTTYPE ,SRCEBUS ,OCCDATE ,SUBSTRING(CAST(OCCDATE AS VARCHAR),1,6) OCCYEAR,AGNTNUM 
		from STAG_ID.STAG_LIFEASIA_CHDRPF 
		where 
		CHDRNUM in (select distinct POLICY_ID from #ga_allowance)
		--order by TRANNO desc
		) a) a where rank = 1
		;
		
		--drop table IF EXISTS #hpad;
		select distinct CHDRNUM ,HOISSDTE ,SUBSTRING(CAST(HOISSDTE AS VARCHAR),1,6) OCCYEAR 
		into #hpad
		from STAG_ID.STAG_LIFEASIA_HPADPF
		where CHDRNUM in (select distinct POLICY_ID from #ga_allowance)
		and VALIDFLAG = '1'
		;
		
		--drop table IF EXISTS #covr;
		select a.CHDRNUM,max(a.CRTABLE) CRTABLE, sum(a.SUMINS) SUMINS 
		into #covr
		from (
		select CHDRNUM ,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end CRTABLE , sum(SUMINS ) SUMINS 
		from STAG_ID.STAG_LIFEASIA_COVRPF 
		where VALIDFLAG = '1'
		and CHDRNUM in (select distinct POLICY_ID from #ga_allowance)
		group by CHDRNUM,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end
		union all
		select CHDRNUM,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end CRTABLE, sum(SUMINS) SUMINS 
		from STAG_ID.STAG_LIFEASIA_COVTPF 
		where  CHDRNUM in (select distinct POLICY_ID from #ga_allowance)
		group by CHDRNUM,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end
		) a group by a.CHDRNUM
		;
		
		--drop table IF EXISTS #T0_2;
		select distinct
		PRODUCT_CD ,T0 
		into #T0_2
		from FOND_ID.FOND_ETL5_LIFEASIA_MASTER_T0 
		where (
		ACCT_PERIOD  
		BETWEEN 
		(YEAR(DATEADD(month, -1,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, -1,CONVERT(date, @batch)))) 
				AND 
		YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 1,CONVERT(date, @batch))) 
		OR ACCT_PERIOD = '9999999')
		and T0 not in ('-','0000LAS','ITYT000','0000000')
		;
		
		--drop table IF EXISTS #FINAL;
		select distinct
		SUBSTRING(CAST(a.DATE_TO AS VARCHAR),1,4)+'0'+SUBSTRING(CAST(a.DATE_TO AS VARCHAR),5,2) AS ACCOUNTING_PERIOD
		,SUBSTRING(CAST (a.DATE_TO AS VARCHAR),1,4)+'-'+SUBSTRING(CAST(a.DATE_TO AS VARCHAR),5,2)+'-'+SUBSTRING(CAST(a.DATE_TO AS VARCHAR),7,2) AS REPORTING_PERIOD
		,a.POLICY_ID AS POLICY_NO
		,c.CRTABLE AS BENF_CD
		,b.CNTTYPE AS PROD_CD
		,e.T0 AS ADJ_T0
		,b.SRCEBUS AS DIST_CHAN
		,d.ga_allowance
		INTO #FINAL
		from (
		select POLICY_ID, DATE_TO from #ga_allowance --group by policy_id
		where substring(CAST(DATE_FROM AS VARCHAR),1,6) = SUBSTRING( CAST (@batch AS VARCHAR),1,6)
		/*
		select substr(date_to,1,4)||'-'||substr(date_to,5,2)||'-'||substr(date_to,7,2) reporting_period,*
		    from ifrs17.etl5_ifrs17_ga_allowance where policy_id = '11775265' and date_from = '20190101' order by sequence desc limit 10
		*/
		)a    
		left join #chdr b on a.POLICY_ID = b.CHDRNUM
		left join #covr c on a.POLICY_ID = c.CHDRNUM
		left join #ga_allowance d on a.POLICY_ID = d.POLICY_ID and a.DATE_TO = d.DATE_TO
		left join (SELECT row_number () OVER (PARTITION BY PRODUCT_CD order by T0 desc) RN , * FROM #T0_2) e on b.CNTTYPE = e.PRODUCT_CD AND e.RN = 1
		
		;
		
		SELECT * 
		INTO #etl5_las_ga_allowance_driver
		FROM #FINAL
		;
		
	
		---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_GA_ALLOWANCE] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	 

		-- SELECT * FROM FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_GA_ALLOWANCE
	
		DELETE FROM FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_GA_ALLOWANCE 
		WHERE ACCOUNTING_PERIOD =  YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)));

		 ---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_GA_ALLOWANCE] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);


		INSERT INTO [FOND_ID].[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_GA_ALLOWANCE]
		SELECT CAST(ACCOUNTING_PERIOD AS VARCHAR) AS ACCOUNTING_PERIOD
				,CAST(REPORTING_PERIOD AS VARCHAR) AS REPORTING_PERIOD
				,CAST(POLICY_NO AS VARCHAR) AS POLICY_NO
				,CAST(BENF_CD AS VARCHAR) AS BENF_CD
				,CAST(PROD_CD AS VARCHAR) AS PROD_CD
				,CAST(ADJ_T0 AS VARCHAR) AS ADJ_T0
				,CAST(DIST_CHAN AS VARCHAR) AS DIST_CHAN 
				,ga_allowance,
				@BATCH_MASTER_ID AS BATCH_MASTER_IDL,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				SUBSTRING( CAST(@batch AS VARCHAR),1,6) BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME 
		FROM #etl5_las_ga_allowance_driver
		;
	
		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #etl5_las_ga_allowance_driver) ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_GA_ALLOWANCE'
		,@drivername,@V_TOTAL_ROWS,'MTD',@V_PERIOD);
		
		SELECT 'Total records : ' + @V_PERIOD;

		IF @@TRANCOUNT > 0
        COMMIT;


		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#etl5_las_ga_allowance_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_las_ga_allowance_driver
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
