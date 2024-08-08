CREATE PROC [FOND_ID].[USP_LOAD_ETL5_LIFEASIA_DRIVER_DETAIL_PRODUCTION_PERSISTENCY] @batch [NVARCHAR](100),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 
	--DECLARE @batch [nvarchar](30);
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PRODUCTION_PERSISTENCY';
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

		SET @SCHEMA ='FOND_ID.'
		SET @V_TABLE1 = 'FOND_ID.FOND_LIFEASIA_ETL4_LIFEASIA_' + SUBSTRING(CAST(@batch AS VARCHAR),1,6);
		SET @V_TABLE2 = 'FOND_LIFEASIA_ETL4_LIFEASIA_INVALID_' + SUBSTRING(CAST(@batch AS VARCHAR),1,6);

		SET @V_START_DATE	= convert(date, cast(@batch as varchar)); -- valuation extract date
		PRINT	'Start date :' + convert(varchar,@V_START_DATE,112);
		SET @V_START 	= convert(datetime,getDATE());

		SET @V_DESCRIPTION 	= 'Start ' + @V_FUNCTION_NAME + ' : ' + convert(varchar,@V_START,121);
		PRINT	@V_DESCRIPTION;
		SET @V_SEQNO		= @V_SEQNO + 1;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
		---------------------------- DROP TEMPORARY TABLE ------------------------------
		IF OBJECT_ID('tempdb..#etl5_las_Production_Persistency_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_las_Production_Persistency_driver
		END;

		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE etl5_las_Production_Persistency_driver : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);		 


		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
		with zbncpf as (
		select * from (
			select ACCTYEAR,ACCTMONTH,case when LEN(CAST(CHDRNUM AS VARCHAR)) = 7 then '0' + CAST(CHDRNUM AS VARCHAR) 
									when LEN(CAST(CHDRNUM AS VARCHAR)) = 6 then '00' + CAST(CHDRNUM AS VARCHAR)
									when LEN(CAST(CHDRNUM AS VARCHAR)) = 5 then '000'+ CAST(CHDRNUM AS VARCHAR) else CAST(CHDRNUM AS VARCHAR)
									end CHDRNUM 
									-- ,EFFDATE,FRCDATE,FIELD_TYPE,BNSAMT -- temp solutions for BNSAMT
									,EFFDATE,FRCDATE,FIELD_TYPE,ORIGAMT * 0.45  AS BNSAMT -- use default value getting from most frequent pct
									--SELECT *
									from STAG_ID.STAG_LIFEASIA_ZBNCPF 
									where CAST(ACCTYEAR AS VARCHAR) + '0' +CAST(ACCTMONTH AS VARCHAR) = SUBSTRING(CAST(@batch AS VARCHAR),1,6)
			) a 
			-- POLICY SELECTION
			-- where a.CHDRNUM IN (SELECT DISTINCT [Pol No Dec] FROM FOND_ID.FOND_ETL5_LIFEASIA_POLICY_SELECTION)
			)		
		
		,chdr as (
		select a.* from (
		select row_number () OVER (
			   PARTITION BY CHDRNUM order by TRANNO desc
			) rank,
		a.* from (
		select TRANNO ,CHDRNUM ,CNTTYPE ,SRCEBUS ,OCCDATE ,SUBSTRING(CAST(OCCDATE AS VARCHAR),1,6) OCCYEAR,AGNTNUM 
		from STAG_ID.STAG_LIFEASIA_CHDRPF 
		where 
		CHDRNUM in (select distinct CHDRNUM from zbncpf)
		--order by TRANNO desc
		) a) a where rank = 1
		)
		
		,hpad as (
		select distinct CHDRNUM ,HOISSDTE ,SUBSTRING(CAST(HOISSDTE AS VARCHAR),1,6) OCCYEAR 
		from STAG_ID.STAG_LIFEASIA_HPADPF
		where CHDRNUM in (select distinct CHDRNUM from zbncpf)
		and VALIDFLAG = '1'
		)
		
		,covr as (
		select a.CHDRNUM,max(a.CRTABLE) CRTABLE, sum(a.SUMINS) SUMINS from (
		select CHDRNUM ,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end CRTABLE , sum(SUMINS ) SUMINS 
		from STAG_ID.STAG_LIFEASIA_COVRPF 
		where VALIDFLAG = '1'
		and CHDRNUM in (select distinct CHDRNUM from zbncpf)
		group by CHDRNUM,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end
		union all
		select CHDRNUM,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end CRTABLE, sum(SUMINS) SUMINS 
		from STAG_ID.STAG_LIFEASIA_COVTPF 
		where  CHDRNUM in (select distinct CHDRNUM from zbncpf)
		group by CHDRNUM,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end
		) a group by a.CHDRNUM
		)
		
		
		,T0_2 as (
		
		--SELECT YEAR(DATEADD(month, 0,CONVERT(date, cast('20191201' as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast('20191201' as varchar(4)))))
		-- SELECT * FROM 
		select distinct
		PRODUCT_CD ,T0 
		from FOND_ID.FOND_ETL5_LIFEASIA_MASTER_T0 
		where 
		ACCT_PERIOD  
		BETWEEN 
		YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
				AND 
		YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) 
		OR ACCT_PERIOD = '9999999'
		and T0 not in ('-','0000LAS','ITYT000','0000000')
		
		)
		
		, FINAL as (
			select CAST(YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) AS VARCHAR) ACCOUNTING_PERIOD
			,a.FRCDATE TRANSACTION_DATE
			,a.CHDRNUM POLICY_NO
			,c.CRTABLE BENF_CD
			,b.CNTTYPE PROD_CD
			,d.T0 ADJ_T0
			,b.SRCEBUS DIST_CHAN
			,a.production_bonus PRODUCTION_BONUS
			,a.persistency_bonus PERSISTENCY_BONUS
			from (select ACCTYEAR,ACCTMONTH,max(FRCDATE) FRCDATE,CHDRNUM,sum(production_bonus) production_bonus,sum(persistency_bonus) persistency_bonus from (
			--select distinct ACCTYEAR,ACCTMONTH,EFFDATE, FRCDATE,CHDRNUM,sum(case when FIELD_TYPE = 'R' then BNSAMT else 0 end) production_bonus -- temp solutions for LSIT
			select distinct ACCTYEAR,ACCTMONTH,EFFDATE, FRCDATE,CHDRNUM,sum(case when FIELD_TYPE = 'P' then BNSAMT else 0 end) production_bonus
									             ,sum(case when FIELD_TYPE = 'P' then BNSAMT else 0 end) persistency_bonus
									from zbncpf
									group by ACCTYEAR,ACCTMONTH,CHDRNUM
									,case when FIELD_TYPE = 'R' then BNSAMT else 0 end
									,case when FIELD_TYPE = 'P' then BNSAMT else 0 end
									,FRCDATE
									,EFFDATE
			) a group by ACCTYEAR,ACCTMONTH,CHDRNUM
									) a						
			left join chdr b on a.CHDRNUM = b.CHDRNUM
			left join covr c on a.CHDRNUM = c.CHDRNUM
			left join (SELECT row_number () OVER (PARTITION BY PRODUCT_CD order by T0 desc) RN , * FROM T0_2) d on b.CNTTYPE = d.PRODUCT_CD AND d.RN = 1
		)
		
		
		SELECT ACCOUNTING_PERIOD,
			TRANSACTION_DATE,
			POLICY_NO,
			BENF_CD,
			PROD_CD,
			ADJ_T0,
			DIST_CHAN,
			PRODUCTION_BONUS,
			PERSISTENCY_BONUS,
			@BATCH_MASTER_ID AS BATCH_MASTER_IDL,
		  	@BATCH_RUN_ID AS BATCH_RUN_ID,
		  	@JOB_MASTER_ID AS JOB_MASTER_ID,
		  	@JOB_RUN_ID AS JOB_RUN_ID,
			SUBSTRING( CAST(@batch AS VARCHAR),1,6) BATCHDATE,
			GETDATE() ETL_PROCESS_DATE_TIME 
		INTO #etl5_las_Production_Persistency_driver
		FROM FINAL;
		
		---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PRODUCTION_PERSISTENCY] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	 

		-- SELECT DISTINCT DRIVER_PERIOD FROM FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PRODUCTION_PERSISTENCY
	
		DELETE FROM FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PRODUCTION_PERSISTENCY 
		WHERE ACCOUNTING_PERIOD =  YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)));

		 ---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PRODUCTION_PERSISTENCY] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);


		INSERT INTO [FOND_ID].[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PRODUCTION_PERSISTENCY]
		SELECT * FROM #etl5_las_Production_Persistency_driver
		;
	
		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #etl5_las_Production_Persistency_driver) ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PRODUCTION_PERSISTENCY'
		,@drivername,@V_TOTAL_ROWS,'MTD',@V_PERIOD);
		
		SELECT 'Total records : ' + @V_PERIOD;

		IF @@TRANCOUNT > 0
        COMMIT;


		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#etl5_las_Production_Persistency_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_las_Production_Persistency_driver
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

