CREATE PROC [FOND_ID].[USP_LOAD_ETL5_IAI_CAT_PORT_CONFIG_SPLITTING] @batch [NVARCHAR](100),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 
	--DECLARE @batch [nvarchar](30);
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_ETL5_IAI_CAT_PORT_CONFIG_SPLITTING';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @drivername NVARCHAR(15);
	SET @drivername = 'IAI_CAT_PORT_CONFIG';
	declare @year varchar(12)=substring(@batch,0,5);
	declare @diff numeric(28,6);
 	declare @firstrow varchar(200);
	
	------ START ABC ------
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
		SET @V_START_DATE	= convert(date, cast(@batch as varchar)); -- valuation extract date
		PRINT	'Start date :' + convert(varchar,@V_START_DATE,112);
		SET @V_START 	= convert(datetime,getDATE());

		SET @V_DESCRIPTION 	= 'Start ' + @V_FUNCTION_NAME + ' : ' + convert(varchar,@V_START,121);
		PRINT	@V_DESCRIPTION;
		SET @V_SEQNO		= @V_SEQNO + 1;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
		IF OBJECT_ID('tempdb..#PORT_CONFIG') IS NOT NULL
		BEGIN
			DROP TABLE #PORT_CONFIG
		END;

		IF OBJECT_ID('tempdb..#PORT_CONFIG_FINAL') IS NOT NULL
		BEGIN
			DROP TABLE #PORT_CONFIG_FINAL
		END;

---IT EXTRACT

--declare @batch varchar(12)='20210301';
--declare @year varchar(12)=substring(@batch,0,5);

		with ga as (
			select
				a.*
			from STAG_ID.STAG_LIFEASIA_GA_ALLOWANCE a
			inner join STAG_ID.STAG_CONFIG_STAG_PRE_COVERAGE_ETL5_GA_CONFIG b on a.GA_SALES_CODE_UNIT = b.GA_CODE
			where left(cast(DATE_FROM as varchar), 6) = left(@batch, 6) 
		),
		cal as(
			select
				a.POLICY_ID,
				b.*
			from (
				select
					a.*,
					'ga' as lbl
				from (
					select
						b.HOISSDTE,
						case when substring(cast(HOISSDTE as varchar), 1, 6) = substring(cast(DATE_FROM as varchar), 1, 6) then 1 else 0 end NB_FLAG ,
						a.*
					from ga a
					left join STAG_ID.STAG_LIFEASIA_HPADPF b on a.POLICY_ID = b.CHDRNUM 
				) a
				where NB_FLAG = 1 
			) a
			left join (
				select
					*,
					case when SUBSTRING(PORTFOLIO_GROUP,3,3) = 'CON' THEN 'IAC' WHEN SUBSTRING(PORTFOLIO_GROUP,3,3) = 'SHA' THEN 'IAS' END AS SHARIA_FLAG,				
					'icg' as lbl
				from FOND_ID.FOND_IFRS17_ICG_STORES
				where PRODUCT_CD = BENEFIT_CD
			) b on a.POLICY_ID = b.POLICY_NO 
		) ,
		tot_ as (
			select
				SHARIA_FLAG,
				count(*) COUNT_TOT,
				COHORT_YEAR
			from cal
			where COHORT_YEAR = @year
			group by SHARIA_FLAG,COHORT_YEAR
		) ,
		icg as (
			select
				SUB_GROUP_ID,
				INSURANCE_CONTRACT_GROUP_ID,
				PRODUCT_CD,
				PORTFOLIO_GROUP,
				COHORT_YEAR,
				ICG_ID_PROPHET,
				SHARIA_FLAG,
				COUNT(POLICY_ID) as COUNT_POL_NO,
				COUNT_TOT
			from (
				SELECT
					a.POLICY_ID,
					a.COHORT_YEAR,
					ICG_ID_PROPHET ,
					PRODUCT_CD ,
					PORTFOLIO_GROUP ,
					a.SHARIA_FLAG,
					a.INSURANCE_CONTRACT_GROUP_ID,
					b.COUNT_TOT as COUNT_TOT,
					n.INSURANCE_CONTRACT_GROUP_ID+'_'+upper(convert(char(3),cast(cast(n.COHORT_YEAR as VARCHAR(9))+'-'+cast(n.ENTRY_MONTH as VARCHAR(9))+'-28' as date),0)) SUB_GROUP_ID
				from (
					select * from cal
					where COHORT_YEAR = @year
				) a
				left join tot_ b on a.COHORT_YEAR = b.COHORT_YEAR AND a.SHARIA_FLAG = b.SHARIA_FLAG
				left join (
					select
						distinct POLICY_NO POLICY_NUMBER,
						SUB_GROUP_ID,
						INSURANCE_CONTRACT_GROUP_ID,ENTRY_MONTH,COHORT_YEAR
					from FOND_ID.FOND_IFRS17_BENEFIT_INSURANCE_CONTRACT_GROUP_MASTER 
						--only for dev
	--					LOCSIT_ABST_BENEFIT_INSURANCE_GROUP_20190131_20201113_TEMP_FOR_DEV ) n on
						--for prod change to this
				) n on a.POLICY_ID = n.POLICY_NUMBER and a.INSURANCE_CONTRACT_GROUP_ID = n.INSURANCE_CONTRACT_GROUP_ID 
			) d
			group by
				INSURANCE_CONTRACT_GROUP_ID,
				PRODUCT_CD,
				PORTFOLIO_GROUP,
				COHORT_YEAR,
				ICG_ID_PROPHET,
				COUNT_TOT,
				SUB_GROUP_ID,SHARIA_FLAG 
		) ,
		extraction as (
			select
				a.*,
				cast(COUNT_POL_NO as numeric(28,6))/ cast(COUNT_TOT as numeric(28, 6))* 100 as WEIGHTED_VALUE
			from (
				select distinct *
				from icg
			) a where SUB_GROUP_ID is not null 
		)
	select * 
	into #PORT_CONFIG
	from (
		select 
			SHARIA_FLAG AS ENTITY_ID,
			'CAT1' PCA_CATEGORY,
			PORTFOLIO_GROUP,
			cast(sum(WEIGHTED_VALUE) as numeric(28,5)) PORTOFOLIO_RATIO,
			'I' [ACTION],
			'ETL5' USER_PROFILE,
			convert(varchar,cast(GETDATE() as date),112) UPDATE_DATE 
		from extraction 
		group by SHARIA_FLAG, PORTFOLIO_GROUP
	) m;

	
	----------------------------FORCED BALANCING---------------------------------------

	declare @diff_portfolio_ratio_sha float = (select sum(PORTOFOLIO_RATIO)-100 from #PORT_CONFIG where PCA_CATEGORY='CAT1' and ENTITY_ID = 'IAS');
	declare @portfolio_grp_sha varchar(100) = (select top 1 PORTFOLIO_GROUP from #PORT_CONFIG where PCA_CATEGORY='CAT1' and ENTITY_ID = 'IAS');

	update #PORT_CONFIG
	set PORTOFOLIO_RATIO = PORTOFOLIO_RATIO - @diff_portfolio_ratio_sha
	where PORTFOLIO_GROUP = @portfolio_grp_sha and PCA_CATEGORY='CAT1' and ENTITY_ID = 'IAS';
	
	declare @diff_portfolio_ratio_con float = (select sum(PORTOFOLIO_RATIO)-100 from #PORT_CONFIG where PCA_CATEGORY='CAT1' and ENTITY_ID = 'IAC');
	declare @portfolio_grp_con varchar(100) = (select top 1 PORTFOLIO_GROUP from #PORT_CONFIG where PCA_CATEGORY='CAT1' and ENTITY_ID = 'IAC');

	update #PORT_CONFIG
	set PORTOFOLIO_RATIO = PORTOFOLIO_RATIO - @diff_portfolio_ratio_con
	where PORTFOLIO_GROUP = @portfolio_grp_con and PCA_CATEGORY='CAT1' and ENTITY_ID = 'IAC';	

	select *
	into #PORT_CONFIG_FINAL
    from (
		select 
			curr.*,
			case when prev.PORTFOLIO_GROUP is null then 'I' when curr.PORTOFOLIO_RATIO <> prev.PORTOFOLIO_RATIO then 'U'
				when prev.PORTFOLIO_GROUP is not null and curr.PORTOFOLIO_RATIO = prev.PORTOFOLIO_RATIO then 'SKIPPED'
			end as [ACTION]
		from (select ENTITY_ID, PCA_CATEGORY, PORTFOLIO_GROUP, PORTOFOLIO_RATIO, USER_PROFILE, UPDATE_DATE from #PORT_CONFIG where PCA_CATEGORY='CAT1') as curr
		left join (select ENTITY_ID, PCA_CATEGORY, PORTFOLIO_GROUP, PORTOFOLIO_RATIO, USER_PROFILE, UPDATE_DATE from [FOND_ID].[FOND_ETL5_IAI_CAT_PORT_CONFIG]  where PCA_CATEGORY='CAT1' and BATCHDATE = LEFT(CONVERT(varchar, DATEADD(MONTH,-1,cast(@batch as date)),112),6) and ACTION <> 'D' AND ENTITY_ID IN ('IAC','IAS') ) as prev 
		on curr.PORTFOLIO_GROUP = prev.PORTFOLIO_GROUP and curr.ENTITY_ID = prev.ENTITY_ID
      
		UNION ALL
      
		select 
			prev.*,
			case when curr.PORTFOLIO_GROUP is null then 'D' end as [ACTION]
		from (select ENTITY_ID, PCA_CATEGORY, PORTFOLIO_GROUP, PORTOFOLIO_RATIO, USER_PROFILE, UPDATE_DATE from #PORT_CONFIG where PCA_CATEGORY='CAT1') as curr
		right join (select ENTITY_ID, PCA_CATEGORY, PORTFOLIO_GROUP, PORTOFOLIO_RATIO, USER_PROFILE, UPDATE_DATE from [FOND_ID].[FOND_ETL5_IAI_CAT_PORT_CONFIG] where PCA_CATEGORY='CAT1' and BATCHDATE = LEFT(CONVERT(varchar, DATEADD(MONTH,-1,cast(@batch as date)),112),6) and ACTION <> 'D' AND ENTITY_ID IN ('IAC','IAS') ) as prev
		on curr.PORTFOLIO_GROUP = prev.PORTFOLIO_GROUP and curr.ENTITY_ID = prev.ENTITY_ID
    ) z where z.[ACTION] is not null and z.[ACTION] <> 'SKIPPED';
   
   -------------------------------CAT2 PROCESS------------------------
   --	declare @batch varchar(20)='20210301'
    declare @updt_date varchar(20)=(select top 1 UPDATE_DATE from #PORT_CONFIG)
---- IAC	
	declare @cat2check1_iac varchar(100)=(
		select 	count(distinct lbl) 
		from (
			select 'precov' lbl from FOND_ID.FOND_ETL5_PRE_COVERAGE where ALLOC_SEGMENT_ID like '%CAT2%' and left(BATCHDATE,4)=left(@batch,4) and ENTITY_ID = 'IAC'
			union
			select 'runoff' lbl from FOND_ID.FOND_ETL5_PRE_COVERAGE_RUNOFF where INSURANCE_SEGMENT_ID like '%CAT2%' and left(BATCHDATE,4)=left(@batch,4) and ENTITY_ID = 'IAC'
		) b
	)
	
	declare @cat2check2_iac varchar(100)=(
		select count(*) from FOND_ID.FOND_ETL5_IAI_CAT_PORT_CONFIG where PCA_CATEGORY='CAT2' and cast(BATCHDATE as int)<cast(@batch as int) AND ENTITY_ID = 'IAC'
	)
	
	if @cat2check1_iac=2 and @cat2check2_iac=0
		insert into #PORT_CONFIG_FINAL 
		values('IAC','CAT2','PRCONYRTIDR_GMM',100,'ETL5',@updt_date,'I')
	ELSE 
		select 'insert CAT2 is not required'
		
---- IAS		
	declare @cat2check1_ias varchar(100)=(
		select 	count(distinct lbl) 
		from (
			select 'precov' lbl from FOND_ID.FOND_ETL5_PRE_COVERAGE where ALLOC_SEGMENT_ID like '%CAT2%' and left(BATCHDATE,4)=left(@batch,4) and ENTITY_ID = 'IAS'
			union
			select 'runoff' lbl from FOND_ID.FOND_ETL5_PRE_COVERAGE_RUNOFF where INSURANCE_SEGMENT_ID like '%CAT2%' and left(BATCHDATE,4)=left(@batch,4) and ENTITY_ID = 'IAS'
		) b
	)
	
	declare @cat2check2_ias varchar(100)=(
		select count(*) from FOND_ID.FOND_ETL5_IAI_CAT_PORT_CONFIG where PCA_CATEGORY='CAT2' and cast(BATCHDATE as int)<cast(@batch as int) AND ENTITY_ID = 'IAS'
	)
	
	if @cat2check1_ias=2 and @cat2check2_ias=0
		insert into #PORT_CONFIG_FINAL 
		values('IAS','CAT2','PRCONYRTIDR_GMM',100,'ETL5',@updt_date,'I')
	ELSE 
		select 'insert CAT2 is not required'		

	---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.[FOND_ETL5_IAI_CAT_PORT_CONFIG] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	 

		DELETE FROM FOND_ID.FOND_ETL5_IAI_CAT_PORT_CONFIG 
		WHERE BATCHDATE =  left( @batch,6) and PCA_CATEGORY='CAT1' and ENTITY_ID IN ('IAS','IAC');
	
		if (select count(*) from #PORT_CONFIG_FINAL where PCA_CATEGORY='CAT2')>0
	 		DELETE FROM FOND_ID.FOND_ETL5_IAI_CAT_PORT_CONFIG 
			WHERE BATCHDATE =  left( @batch,6) and PCA_CATEGORY='CAT2' and ENTITY_ID IN ('IAS','IAC');
		ELSE 
		  select 'deletion CAT2 is not required'

	---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_IAI_CAT_PORT_CONFIG] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	

		insert into FOND_ID.FOND_ETL5_IAI_CAT_PORT_CONFIG
		(ENTITY_ID, PCA_CATEGORY, PORTFOLIO_GROUP, PORTOFOLIO_RATIO, [ACTION], USER_PROFILE, UPDATE_DATE, BATCH_MASTER_ID, BATCH_RUN_ID, JOB_MASTER_ID, JOB_RUN_ID, BATCHDATE, ETL_PROCESS_DATE_TIME)	
		select
			ENTITY_ID,
			PCA_CATEGORY,
			PORTFOLIO_GROUP,
			PORTOFOLIO_RATIO,
			[ACTION],
			USER_PROFILE,
			case when a.UPDATE_DATE is null then b.UPDATE_DATE else a.UPDATE_DATE end UPDATE_DATE,
			@BATCH_MASTER_ID AS BATCH_MASTER_ID,
			@BATCH_RUN_ID AS BATCH_RUN_ID,
			@JOB_MASTER_ID AS JOB_MASTER_ID,
			@JOB_RUN_ID AS JOB_RUN_ID,
			left(@batch,6) BATCHDATE,
			GETDATE() ETL_PROCESS_DATE_TIME
		from #PORT_CONFIG_FINAL a
		left join (select top 1 ENTITY_ID ENTITY_ID_B,UPDATE_DATE from #PORT_CONFIG_FINAL where UPDATE_DATE is not null) b on a.ENTITY_ID=b.ENTITY_ID_B
		
	
	---------------------------- ETL5 LOGGING ---------------------------- 	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #PORT_CONFIG) ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_IAI_CAT_PORT_CONFIG'
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

