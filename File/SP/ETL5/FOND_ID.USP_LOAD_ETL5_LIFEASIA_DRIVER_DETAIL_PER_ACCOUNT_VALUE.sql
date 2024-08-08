CREATE PROC [FOND_ID].[USP_LOAD_ETL5_LIFEASIA_DRIVER_DETAIL_PER_ACCOUNT_VALUE] @batch [NVARCHAR](100),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 
	--DECLARE @batch [nvarchar](30);
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.USP_LOAD_ETL5_LIFEASIA_DRIVER_DETAIL_PER_ACCOUNT_VALUE';
	DECLARE @V_TABLE1 		NVARCHAR(2000);
	DECLARE @V_TABLE2 		NVARCHAR(2000);
	DECLARE @SCHEMA      NVARCHAR(MAX);
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @drivername NVARCHAR(15);
	DECLARE @EXTRACT_YEAR VARCHAR(4);
	DECLARE @EXTRACT_MONTH VARCHAR(2);
	DECLARE @V_SQL NVARCHAR(MAX);
	DECLARE @BATCHDATE_ETL2A NVARCHAR(6);
	--DECLARE @JOBNAMESTR VARCHAR(2000);
	SET @drivername = 'ACCOUNTVALUE';

	SET	@EXTRACT_YEAR = YEAR(@batch);
	SET @EXTRACT_MONTH = FORMAT(MONTH(@batch), '00');
	SET @BATCHDATE_ETL2A = CONCAT(@EXTRACT_YEAR, @EXTRACT_MONTH);
	--SET @JOBNAMESTR = 'PKG_PRC_FOND_PLAI_UNDERLYING_ITEM_BALANCE';
	DECLARE
    @BATCH_MASTER_ID    VARCHAR(20) = 0,
    @BATCH_RUN_ID       VARCHAR(20) = 0,
    @JOB_MASTER_ID      VARCHAR(20) = 0,
    @JOB_RUN_ID         VARCHAR(20) = 0,
    @GMT_START_DTTM     VARCHAR(20) = GETDATE();
	
	EXEC STAG_ID.USP_GetRunIdReturn
	  @JobName        = @JOBNAMESTR,
	  @BATCH_MASTER_ID = @BATCH_MASTER_ID OUTPUT,
	  @BATCH_RUN_ID    = @BATCH_RUN_ID OUTPUT,
	  @JOB_MASTER_ID   = @JOB_MASTER_ID OUTPUT,
	  @JOB_RUN_ID      = @JOB_RUN_ID OUTPUT,
	  @GMT_START_DTTM  = @GMT_START_DTTM OUTPUT;
	------END GET RUN ID DETAIL FROM ABC--------- 	
 

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
		IF OBJECT_ID('tempdb..#etl5_las_per_account_value_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_las_per_account_value_driver
		END;

		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE etl5_las_per_account_value_driver : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);		 

		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#transition_account_value') IS NOT NULL
	    BEGIN
	    	DROP TABLE #transition_account_value
	    END;
	    
	    IF OBJECT_ID('tempdb..#transition_account_value_chdr') IS NOT NULL
	    BEGIN
	    	DROP TABLE #transition_account_value_chdr
	    END;
	    
	    IF OBJECT_ID('tempdb..#transition_account_value_hpad') IS NOT NULL
	    BEGIN
	    	DROP TABLE #transition_account_value_hpad
	    END;
	    
	    IF OBJECT_ID('tempdb..#transition_account_value_utrs') IS NOT NULL
	    BEGIN
	    	DROP TABLE #transition_account_value_utrs
	    END;
	    
	    IF OBJECT_ID('tempdb..#transition_account_value_vprc') IS NOT NULL
	    BEGIN
	    	DROP TABLE #transition_account_value_vprc
	    END;
	    
	    IF OBJECT_ID('tempdb..#transition_temp_account_value') IS NOT NULL
	    BEGIN
	    	DROP TABLE #transition_temp_account_value
	    END;

		IF OBJECT_ID('tempdb..#T0') IS NOT NULL
	    BEGIN
	    	DROP TABLE #T0
	    END;

		--exec [FOND_ID].[USP_LOAD_ETL2_UNDERLYING_ITEM_BALANCE] @BATCHDATE_ETL2A, @JOBNAMESTR;

		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
	--TEMP TABLE transition_account_value_chdr
	select b.*
	into #transition_account_value_chdr
	from
	  (
	    select
	      a.*,
	      row_number () OVER (PARTITION BY CHDRNUM order by TRANNO desc) rank
	    from
	      (
	    	  select 
	    		TRANNO,
	    		CHDRNUM,
	    		CNTTYPE,
	    		SRCEBUS,
	    		OCCDATE,
	    		convert(numeric,substring(convert(varchar,OCCDATE),1,4)) OCCYEAR,
	    		AGNTNUM, 
	    		CNTCURR, 
	    		PTDATE, 
	    		BILLFREQ
	    	  from STAG_ID.STAG_LIFEASIA_CHDRPF 
	      ) a
	  ) b
	where b.rank = 1;

	--TEMP TABLE #transition_account_value_hpad
    select distinct 
	  HPADF.CHDRNUM,
	  HPADF.HOISSDTE,
	  convert(numeric,substring(convert(varchar, HPADF.HOISSDTE),1,4)) AS OCCYEAR 
	into #transition_account_value_hpad
	from  STAG_ID.STAG_LIFEASIA_HPADPF AS HPADF
	inner join (select CHDRNUM from #transition_account_value_chdr group by CHDRNUM) AS CHDR
	on HPADF.CHDRNUM= CHDR.CHDRNUM;

	--TEMP TABLE #transition_account_value_utrs
    select
	  a.CHDRNUM,
	  a.CRTABLE,
	  a.CNTCURR,
	  a.EFFDATE,
	  a.AMT_RPT_CCY
	into #transition_account_value_utrs
	from
	(
	    SELECT
	    POLICY_NO AS CHDRNUM,
	    PRODUCT_CD AS CRTABLE,
	    RPT_CCY_CD AS CNTCURR,
		TXN_DT AS EFFDATE,
	    AMT_RPT_CCY
	  FROM FOND_ID.FOND_ETL2_UNDERLYING_ITEM_BALANCE_INVALID
	  --FROM FOND_ID.FOND_ETL2_UNDERLYING_ITEM_BALANCE_INVALID_ORI
      WHERE BATCHDATE = CONCAT(@EXTRACT_YEAR, @EXTRACT_MONTH)
      AND PRODUCT_CD IS NOT NULL
        
	  UNION ALL
        
	  SELECT
	    POLICY_NO AS CHDRNUM,
	    PRODUCT_CD AS CRTABLE,
	    RPT_CCY_CD AS CNTCURR,
		TXN_DT AS EFFDATE,
	    AMT_RPT_CCY
	  FROM FOND_ID.FOND_ETL2_UNDERLYING_ITEM_BALANCE
	  --FROM FOND_ID.FOND_ETL2_UNDERLYING_ITEM_BALANCE_ORI
      WHERE BATCHDATE = CONCAT(@EXTRACT_YEAR, @EXTRACT_MONTH)
	) a;

	--TEMP TABLE #transition_temp_account_value
	select a.*
	into #transition_temp_account_value
	from
	  (
	  	select
		  CHDRNUM,
		  CNTCURR,
		  CRTABLE,
		  max(EFFDATE) AS EFFDATE,
		  sum(AMT_RPT_CCY) AS PER_ACCOUNT_VALUE 
	  	from #transition_account_value_utrs
	  	group by CHDRNUM, CNTCURR, CRTABLE
	  ) a;

	--TEMP TABLE #transition_account_value
	select
	  CONCAT(YEAR(a.EFFDATE), FORMAT(MONTH(a.EFFDATE), '000')) AS ACCOUNTING_PERIOD,
	  a.EFFDATE AS TRANSACTION_DATE,
	  a.CHDRNUM AS POLICY_NO,
	  a.CRTABLE AS BENF_CD_ORIGINAL,
	  COALESCE(c.CRTABLE,d.CRTABLE) AS BENF_CD,
	  b.CNTTYPE AS PROD_CD,
	  b.SRCEBUS AS DIST_CHAN,
	  a.CNTCURR,
	  a.PER_ACCOUNT_VALUE,
	  c.CRRCD,
	  b.PTDATE,
	  b.BILLFREQ,
	  e.HOISSDTE
	into #transition_account_value
	from #transition_temp_account_value AS a 
	left join #transition_account_value_chdr AS b on a.CHDRNUM = b.CHDRNUM
	left join STAG_ID.STAG_LIFEASIA_COVRPF AS c on a.CHDRNUM = c.CHDRNUM and concat(c.LIFE,c.COVERAGE,c.RIDER) = '010100' and VALIDFLAG = '1'
	left join STAG_ID.STAG_LIFEASIA_COVTPF AS d on a.CHDRNUM = d.CHDRNUM and concat(d.LIFE,d.COVERAGE,d.RIDER) = '010100' 
	left join #transition_account_value_hpad AS e on a.CHDRNUM = e.CHDRNUM;

	--TEMP TABLE T0
	SELECT DISTINCT
	  PRODUCT_CD,
	  T0 
	INTO #T0
	FROM FOND_ID.FOND_ETL5_LIFEASIA_MASTER_T0 
	where ACCT_PERIOD BETWEEN YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) AND YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) 
	  OR ACCT_PERIOD = '9999999'
	  AND T0 NOT IN ('-','0000LAS','ITYT000','0000000');

	--TEMP TABLE #transition_account_value
	UPDATE #transition_account_value
	SET BENF_CD = CASE
	                WHEN SUBSTRING(BENF_CD,4,1) = '1' THEN CONCAT(SUBSTRING(BENF_CD,1,3),'R') 
		            WHEN SUBSTRING(BENF_CD,4,1) = '2' THEN CONCAT(SUBSTRING(BENF_CD,1,3),'D')
		            ELSE BENF_CD 
		          END 
	WHERE BENF_CD IN ('U1U1', 'U241', 'U251', 'U221', 'U2V1', 'U2T1', 'U2T2', 'U2U1', 'U2U2', 'U2V1', 'U2V2', 'U2W1', 'U2W2');

	---------------------------- TO Handle rerun process ------------------------------
	BEGIN TRANSACTION;
	SET @V_SEQNO 	= @V_SEQNO + 1;
	SET @V_START 	= convert(datetime,getDATE());
	SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_ACCOUNT_VALUE : ' + convert(varchar,@V_START,121);
	PRINT @V_DESCRIPTION;

	INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
	VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

	--DELETE EXPECTED ACCOUNTING_PERIOD FROM TABLE FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_ACCOUNT_VALUE
	DELETE FROM FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_ACCOUNT_VALUE
	WHERE ACCOUNTING_PERIOD =  YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)));

	---------------------------- TO Handle rerun process ------------------------------
	SET @V_SEQNO 	= @V_SEQNO + 1;
	SET @V_START 	= convert(datetime,getDATE());
	SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_ACCOUNT_VALUE] : ' + convert(varchar,@V_START,121);
	PRINT @V_DESCRIPTION;

	INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
	VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
		
	INSERT INTO FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_ACCOUNT_VALUE
	SELECT
	  TRANSITION_ACCOUNT_VALUE.ACCOUNTING_PERIOD,
	  TRANSITION_ACCOUNT_VALUE.TRANSACTION_DATE,
	  TRANSITION_ACCOUNT_VALUE.POLICY_NO,
	  TRANSITION_ACCOUNT_VALUE.BENF_CD,
	  TRANSITION_ACCOUNT_VALUE.PROD_CD,
	  LIFEASIA_MASTER_T0.T0 AS ADJ_T0,
	  TRANSITION_ACCOUNT_VALUE.DIST_CHAN,
	  TRANSITION_ACCOUNT_VALUE.PER_ACCOUNT_VALUE,
	  @BATCH_MASTER_ID AS BATCH_MASTER_ID,
	  @BATCH_RUN_ID AS BATCH_RUN_ID,
	  @JOB_MASTER_ID AS JOB_MASTER_ID,
	  @JOB_RUN_ID AS JOB_RUN_ID,
	  SUBSTRING(@batch,1,6) AS BATCHDATE,
	  GETDATE() AS ETL_PROCESS_DATE_TIME
	FROM #transition_account_value AS TRANSITION_ACCOUNT_VALUE
	LEFT JOIN (SELECT row_number () OVER (PARTITION BY PRODUCT_CD order by T0 desc) RN , * FROM #T0) LIFEASIA_MASTER_T0 
	ON TRANSITION_ACCOUNT_VALUE.PROD_CD = LIFEASIA_MASTER_T0.PRODUCT_CD AND LIFEASIA_MASTER_T0.RN = 1

	---------------------------- ETL5 LOGGING ----------------------------      
     
	DECLARE @V_TOTAL_ROWS integer = 0;
	DECLARE @V_PERIOD nvarchar(10);
	SET @V_TOTAL_ROWS = (SELECT COUNT(*) FROM #transition_account_value);
    SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

	INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
	VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_ACCOUNT_VALUE',@drivername,@V_TOTAL_ROWS,'MTD',@V_PERIOD);
	
	SELECT 'Total records : ' + CAST(@V_TOTAL_ROWS AS VARCHAR(50));

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
