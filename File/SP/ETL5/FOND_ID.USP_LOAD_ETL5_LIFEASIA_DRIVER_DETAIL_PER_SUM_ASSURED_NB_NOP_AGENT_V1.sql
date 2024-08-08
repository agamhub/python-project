CREATE PROC [FOND_ID].[USP_LOAD_ETL5_LIFEASIA_DRIVER_DETAIL_PER_SUM_ASSURED_NB_NOP_AGENT_V1] @batch [NVARCHAR](100) AS
BEGIN 
	--DECLARE @batch [nvarchar](30);
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_SUM_ASSURED_NB_NOP_AGENT_V1';
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
	SET @drivername = 'LASSUMASSURED';
 

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
		IF OBJECT_ID('tempdb..#etl5_las_per_sum_assured_nb_nop_agent_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_las_ape_driver
		END;

		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE etl5_las_per_sum_assured_nb_nop_agent_driver : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);		 


		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
		with chdr as (
		select a.* from (
		select RANK () OVER (
			   PARTITION BY CHDRNUM order by TRANNO desc
			) rank,
		a.* from (
		select TRANNO ,CHDRNUM ,CNTTYPE ,SRCEBUS ,OCCDATE ,SUBSTRING(CAST(OCCDATE AS VARCHAR),1,6) OCCYEAR,AGNTNUM
		,STATCODE --new
		from STAG_ID.STAG_LIFEASIA_CHDRPF 
		where 
		 --VALIDFLAG = 1 and (remove based on transition)
		 convert(char(8),EOMONTH(convert(datetime, convert(varchar,@batch),112)),112) between CURRFROM and CURRTO --new
		--order by TRANNO desc
		) a) a where rank = 1 and STATCODE = 'IF'
		--POLICY SELECTION
		--AND a.CHDRNUM IN (SELECT DISTINCT [Pol No Dec] FROM FOND_ID.FOND_ETL5_LIFEASIA_POLICY_SELECTION)
		)
		
		,hpad as (
		select distinct CHDRNUM ,HOISSDTE ,SUBSTRING(CAST(HOISSDTE AS VARCHAR),1,6) OCCYEAR 
		from STAG_ID.STAG_LIFEASIA_HPADPF
		where CHDRNUM in (select distinct CHDRNUM from chdr)
		and VALIDFLAG = '1'
		)
		
		,covr as (
		select a.CHDRNUM,max(a.CRTABLE) CRTABLE, sum(a.SUMINS) SUMINS from (
		select CHDRNUM ,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end CRTABLE , sum(SUMINS ) SUMINS 
		from STAG_ID.STAG_LIFEASIA_COVRPF 
		where VALIDFLAG = '1' and LIFE = '01' and COVERAGE = '01' and RIDER = '00'
		and CHDRNUM in (select distinct CHDRNUM from chdr)
		group by CHDRNUM,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end
		union all
		select CHDRNUM,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end CRTABLE, sum(SUMINS) SUMINS 
		from STAG_ID.STAG_LIFEASIA_COVTPF 
		where LIFE = '01' and COVERAGE = '01' and RIDER = '00'
		AND CHDRNUM in (select distinct CHDRNUM from chdr)
		group by CHDRNUM,case when LIFE = '01' and COVERAGE = '01' and RIDER = '00' then CRTABLE else '' end
		) a group by a.CHDRNUM
		)
		
		
		,T0_2 as (
		
		--SELECT YEAR(DATEADD(month, 0,CONVERT(date, cast('20191201' as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast('20191201' as varchar(4)))))
		
		select distinct
		PRODUCT_CD ,T0 
		from FOND_ID.FOND_ETL5_LIFEASIA_MASTER_T0 
		where (
		ACCT_PERIOD  
		BETWEEN 
		YEAR(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4))))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, cast(@batch as varchar(4)))))
				AND 
		YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) 
		OR ACCT_PERIOD = '9999999'
		)
		and T0 not in ('-','0000LAS','ITYT000','0000000')
		
		)
		
		,fin as (
			select
			CAST(YEAR(DATEADD(month, 0,CONVERT(date,@batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch))) AS VARCHAR) ACCOUNTING_PERIOD
			,SUBSTRING(CAST(A.OCCYEAR AS VARCHAR),1,5) + '-'+ SUBSTRING(CAST(A.OCCYEAR AS VARCHAR),5,2) + '-' + SUBSTRING(CAST(A.OCCYEAR AS VARCHAR),7,2) TRANSACTION_DATE
			,A.CHDRNUM POLICY_NO
			,C.CRTABLE BENF_CD
			,A.CNTTYPE PROD_CD
			,D.T0 ADJ_T0
			,A.SRCEBUS DIST_CHAN
			,C.SUMINS PER_SUM_ASSURED
			,A.AGNTNUM
			,case when SUBSTRING(CAST(B.OCCYEAR AS VARCHAR),1,6) = left(@batch,6) then 1 else 0 end NB_FLAG
			,case when A.CHDRNUM is not null and LEFT(B.HOISSDTE,4) <> LEFT(@batch,4) then 1 else 0 end NUMBER_OF_POLICY --new, add clause
			from (select * from chdr where SUBSTRING(CAST (OCCYEAR AS VARCHAR),1,6) <= left(@batch,6)) A 
			left join hpad B on A.CHDRNUM = B.CHDRNUM 
			left join covr C on B.CHDRNUM = C.CHDRNUM
			left join T0_2 D on A.CNTTYPE = D.PRODUCT_CD --and substr(a.hoissdte,1,4)||'0'||substr(a.hoissdte,5,2) = d.acct_period
			-- where LEFT(B.HOISSDTE,4) <> LEFT(@batch,4) --new
			)

			
			,FINAL as (
			select
			    A.*,B.AGTYPE,B.DTETRM, 
			    case when B.AGTYPE = 'Agent' and B.DTETRM=99999999 then 1
					when B.AGTYPE = 'FSC' and B.DTETRM=99999999 then 0
				end AGENT_FLAG
					
			from
			    fin A left join (
			    select AGLFPF.DTETRM ,AGLFPF.AGNTNUM ,
			        case
			            when AGNTPF.AGTYPE in ('AD','MA','SU','UM','AG','AM','AA','AD','HO','HA','RL','C1','ED') then 'Agent'
			            when AGNTPF.AGTYPE in ('MFSC','PF','MFC','AS','PS','FSC','FFSC','FF','PFSC','SM','FC','MF') then 'FSC'
			        end as AGTYPE
			    from
			        STAG_ID.STAG_LIFEASIA_AGLFPF AGLFPF inner join STAG_ID.STAG_LIFEASIA_AGNTPF AGNTPF
			        on AGLFPF.AGNTNUM = AGNTPF.AGNTNUM
			    where 1=1
			        --and AGLFPF.dtetrm=99999999
			        and AGNTPF.AGTYPE in (
			            'AD','MA','SU','UM','AG','AM','AA','AD','HO','HA','RL','C1','ED',
			            'MFSC','PF','MFC','AS','PS','FSC','FFSC','FF','PFSC','SM','FC','MF'   
			            )
			) B
			on A.AGNTNUM = B.AGNTNUM)
		
		
		SELECT ACCOUNTING_PERIOD
				,TRANSACTION_DATE
				,POLICY_NO
				,BENF_CD
				,PROD_CD
				,ADJ_T0
				,DIST_CHAN
				,PER_SUM_ASSURED
				,AGNTNUM
				,NB_FLAG
				,NUMBER_OF_POLICY
				,AGTYPE
				,DTETRM
				,AGENT_FLAG,
				0 BATCH_MASTER_IDL,
				0 BATCH_RUN_ID,
				0 JOB_MASTER_ID,
				0 JOB_RUN_ID,
				SUBSTRING( CAST(@batch AS VARCHAR),1,6) BATCHDATE,
				GETDATE() ETL_PROCESS_DATE_TIME 
		INTO #etl5_las_per_sum_assured_nb_nop_agent_driver
		FROM FINAL;
		
		---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_SUM_ASSURED_NB_NOP_AGENT] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	 

		-- SELECT DISTINCT DRIVER_PERIOD FROM FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_SUM_ASSURED_NB_NOP_AGENT
	
		DELETE FROM FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_SUM_ASSURED_NB_NOP_AGENT 
		WHERE ACCOUNTING_PERIOD =  YEAR(DATEADD(month, 0,CONVERT(date, @batch))) * 1000 + 0 + MONTH(DATEADD(month, 0,CONVERT(date, @batch)));

		 ---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_SUM_ASSURED_NB_NOP_AGENT] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);


		INSERT INTO [FOND_ID].[FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_SUM_ASSURED_NB_NOP_AGENT]
		SELECT * FROM #etl5_las_per_sum_assured_nb_nop_agent_driver
		;
	
		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #etl5_las_per_sum_assured_nb_nop_agent_driver) ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_LIFEASIA_DRIVER_DETAIL_PER_SUM_ASSURED_NB_NOP_AGENT'
		,@drivername,@V_TOTAL_ROWS,'MTD',@V_PERIOD);
		
		SELECT 'Total records : ' + @V_PERIOD;

		IF @@TRANCOUNT > 0
        COMMIT;


		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#etl5_las_per_sum_assured_nb_nop_agent_driver') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_las_per_sum_assured_nb_nop_agent_driver
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

