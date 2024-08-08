CREATE PROC [FOND_ID].[USP_LOAD_ETL5_LIFEASIA_DRIVER_DETAIL_APE] @batch [nvarchar](100),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 
	--DECLARE @batch [nvarchar](30);
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_APE';
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
	SET @drivername = 'LASAPE';
	DECLARE @V_DRIVER_PERIOD VARCHAR(10); 
	SET @V_DRIVER_PERIOD =SUBSTRING(CAST(@batch AS VARCHAR),1,6);
	DECLARE @V_TNAME_EB VARCHAR(100) = 'FOND_ID.FOND_LIFEASIA_ETL4_LIFEASIA_'+@V_DRIVER_PERIOD;
	DECLARE @V_TNAME_EB1 VARCHAR(100) = 'FOND_ID.FOND_LIFEASIA_ETL4_LIFEASIA_INVALID_'+@V_DRIVER_PERIOD;
 
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
		IF OBJECT_ID('tempdb..#etl5_las_ape_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_las_ape_driver
		END;
		IF OBJECT_ID('tempdb..#APE') IS NOT NULL
		BEGIN
			DROP TABLE #APE
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
		IF OBJECT_ID('tempdb..#T0') IS NOT NULL
		BEGIN
			DROP TABLE #T0
		END;
		IF OBJECT_ID('tempdb..#T0_3') IS NOT NULL
		BEGIN
			DROP TABLE #T0_3
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
		--DECLARE @batch [nvarchar](30);
		--SET @batch = '20190101';	
	
		--DROP TABLE #APE;
		SELECT A.* 
		INTO #APE
		FROM (
		select YEAR_NUM,MONTH_NUM_OF_YEAR,case when len(POLICY_NO) = 7 then concat('0',POLICY_NO) 
				when len(POLICY_NO) = 6 then concat('00',POLICY_NO)
				when len(POLICY_NO) = 5 then concat('000',POLICY_NO) else POLICY_NO
				end POLICY_NO
				,CAST (TOT_GROSS_API as numeric) AS TOT_GROSS_API	
				,CAST (PRUSAVER_INCREMENT as numeric) AS PRUSAVER_INCREMENT
				,CAST (TOT_GROSS_SPI10 as numeric)	AS TOT_GROSS_SPI10
				,CAST (TOT_GROSS_TOPUP10 as numeric) AS TOT_GROSS_TOPUP10
				,CAST (PSV_NEWBASIS_BEFORE_2002 as numeric)	AS PSV_NEWBASIS_BEFORE_2002
				,CAST (PSV_NEWBASIS_2002_AFTER as numeric) AS PSV_NEWBASIS_2002_AFTER
				,CAST (TOT_GROSS_API as numeric) -- + CAST (PSV_NEWBASIS_2002_AFTER as numeric) 
				+ CAST (TOT_GROSS_SPI10 as numeric) 
				+ CAST (TOT_GROSS_TOPUP10 as numeric) 
				+ CAST (PSV_NEWBASIS_BEFORE_2002 as numeric) + CAST (PSV_NEWBASIS_2002_AFTER as numeric)  as APE
				-- select *
		
		-- from STAG_ID.STAG_DATAMART_ETL5_APIS
		from STAG_ID.STAG_DATAMART_ETL5_LAND_APIS
		-- WHERE CAST(YEAR_NUM AS VARCHAR) + '0' +CAST(MONTH_NUM_OF_YEAR AS VARCHAR) = SUBSTRING(CAST(@batch AS VARCHAR),1,6)
		WHERE CAST(YEAR_NUM AS VARCHAR) + RIGHT('0' +CAST(MONTH_NUM_OF_YEAR AS VARCHAR),2)  = SUBSTRING(CAST(@batch AS VARCHAR),1,6)
		) A 
		-- POLICY SELECTION
		--WHERE POLICY_NO IN (SELECT DISTINCT [Pol No Dec] FROM FOND_ID.FOND_ETL5_LIFEASIA_POLICY_SELECTION)
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
		CHDRNUM in (select distinct POLICY_NO from #APE)
		--order by TRANNO desc
		) a) a where rank = 1
		;
		
		--drop table IF EXISTS #hpad;
		select distinct CHDRNUM ,HOISSDTE ,SUBSTRING(CAST(HOISSDTE AS VARCHAR),1,6) OCCYEAR 
		into #hpad
		from STAG_ID.STAG_LIFEASIA_HPADPF
		where CHDRNUM in (select distinct POLICY_NO from #APE)
		and VALIDFLAG = '1'
		;
		
		-- SELECT COUNT(*) FROM STAG_ID.STAG_LIFEASIA_COVRPF
		--drop table IF EXISTS #covr;
		select a.CHDRNUM,max(a.CRTABLE) CRTABLE, sum(a.SUMINS) SUMINS 
		into #covr
		from (
		select CHDRNUM ,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end CRTABLE , sum(SUMINS ) SUMINS 
		from STAG_ID.STAG_LIFEASIA_COVRPF 
		where VALIDFLAG = '1'
		and CHDRNUM in (select distinct POLICY_NO from #APE)
		group by CHDRNUM,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end
		union all
		select CHDRNUM,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end CRTABLE, sum(SUMINS) SUMINS 
		from STAG_ID.STAG_LIFEASIA_COVTPF 
		where  CHDRNUM in (select distinct POLICY_NO from #APE)
		group by CHDRNUM,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end
		) a group by a.CHDRNUM
		;
	
		-- SELECT * FROM #covr
		
		--DECLARE @batch [nvarchar](30);
		--SET @batch = '20190101';
		--drop table IF EXISTS #T0_3;
		select distinct
		PRODUCT_CD ,T0 
		into #T0_3
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
		
		--DECLARE @batch [nvarchar](30);
		--SET @batch = '20190101';
		--DECLARE @V_DRIVER_PERIOD VARCHAR(10); 
		--SET @V_DRIVER_PERIOD =SUBSTRING(CAST(@batch AS VARCHAR),1,6);
		--DECLARE @V_TNAME_EB VARCHAR(100) = 'FOND_ID.FOND_LIFEASIA_ETL4_LIFEASIA_'+@V_DRIVER_PERIOD;
		--DECLARE @V_TNAME_EB1 VARCHAR(100) = 'FOND_ID.FOND_LIFEASIA_ETL4_LIFEASIA_INVALID_'+@V_DRIVER_PERIOD;
	 
		-- SELECT * FROM FOND_ID.FOND_LIFEASIA_ETL4_LIFEASIA_201902
		--drop table IF EXISTS #T0;
		DECLARE @sql_selectTEB VARCHAR(8000)=
		'select a.* 
		into #T0
		from (
		select ACCT_PERIOD AS ACCT_PERIOD,TXN_DT AS TXN_DT,POLICY_NO AS POLICY_NO,T0 AS T0,AMT_RPT_CCY AS AMT_RPT_CCY from '+@V_TNAME_EB+' where POLICY_NO in (select distinct POLICY_NO from #APE)
		 union 
		select ACCT_PERIOD,TXN_DT,POLICY_NO,T0,AMT_RPT_CCY from '+@V_TNAME_EB1+' where POLICY_NO in (select distinct POLICY_NO from #APE)
		) a';
		--Create Dynamic table for
		EXEC( @sql_selectTEB);
		
		--drop table IF EXISTS #FINAL;
		select
		CAST(a.YEAR_NUM AS VARCHAR) + '00'+ CAST (a.MONTH_NUM_OF_YEAR AS VARCHAR) ACCOUNTING_PERIOD
		,CAST(a.YEAR_NUM AS VARCHAR) + '-'+ CAST (a.MONTH_NUM_OF_YEAR AS VARCHAR) + '-01' TRANSACTION_DATE
		,a.POLICY_NO POLICY_NO
		,c.CRTABLE BENF_CD
		,b.CNTTYPE PROD_CD
		,g.T0 ADJ_T0
		,b.SRCEBUS DIST_CHAN
		,f.AMT_RPT_CCY AFYP_GROSS
		,a.APE
		--,case when a.policy_no is not null then 1 else 0 end number_of_policy
		into #FINAL
		 from (select YEAR_NUM,MONTH_NUM_OF_YEAR,POLICY_NO,APE from #APE
		--limit 10
		) a
		left join #chdr b on a.POLICY_NO = b.CHDRNUM
		left join #covr c on a.POLICY_NO = c.CHDRNUM
		left join #hpad h on a.POLICY_NO = h.CHDRNUM
		left join (
		select POLICY_NO,ACCT_PERIOD,max(TXN_DT) TRANSACTION_DATE, sum(AMT_RPT_CCY) AMT_RPT_CCY from #T0
						group by POLICY_NO,ACCT_PERIOD
		) f on a.POLICY_NO = f.POLICY_NO
		left join #T0_3 g on b.CNTTYPE = g.PRODUCT_CD
		
		
		
		SELECT * 
		INTO #etl5_las_ape_driver
		FROM #FINAL
		;
		
	
		---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_APE] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	 

		-- SELECT DISTINCT DRIVER_PERIOD FROM FOND_ID.FOND_ETL5_APE_DRIVER_DETAIL
	
		DELETE FROM FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_APE 
		WHERE ACCOUNTING_PERIOD =  YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)));

		 ---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_APE] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);


		INSERT INTO [FOND_ID].[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_APE]
		SELECT ACCOUNTING_PERIOD,
				TRANSACTION_DATE,
				POLICY_NO,
				BENF_CD,
				PROD_CD,
				ADJ_T0,
				DIST_CHAN,
				AFYP_GROSS,
				APE,
				@BATCH_MASTER_ID AS BATCH_MASTER_IDL,
				@BATCH_RUN_ID AS BATCH_RUN_ID,
				@JOB_MASTER_ID AS JOB_MASTER_ID,
				@JOB_RUN_ID AS JOB_RUN_ID,
				SUBSTRING( CAST(@batch AS VARCHAR),1,6) BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME 
		FROM #etl5_las_ape_driver
		WHERE APE != 0
		;
	
		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #etl5_las_ape_driver) ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_APE'
		,@drivername,@V_TOTAL_ROWS,'MTD',@V_PERIOD);
		
		SELECT 'Total records : ' + CAST(@V_TOTAL_ROWS as varchar);

		IF @@TRANCOUNT > 0
        COMMIT;


		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#etl5_las_ape_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_las_ape_driver
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
