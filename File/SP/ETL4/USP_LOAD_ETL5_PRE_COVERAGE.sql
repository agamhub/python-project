CREATE PROC [FOND_ID].[USP_LOAD_ETL5_PRE_COVERAGE] @batch [NVARCHAR](100),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 
	--DECLARE @batch [nvarchar](30);
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.USP_LOAD_ETL5_PRE_COVERAGE';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @drivername NVARCHAR(15);
	SET @drivername = 'PRE_COVERAGE';

	--declare @batch varchar(12)='20190101';
	declare @year varchar(12)=substring(@batch,0,5);
	
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
		SET @V_START_DATE	= convert(date, cast(@batch as varchar)); 
		PRINT	'Start date :' + convert(varchar,@V_START_DATE,112);
		SET @V_START 	= convert(datetime,getDATE());

		SET @V_DESCRIPTION 	= 'Start ' + @V_FUNCTION_NAME + ' : ' + convert(varchar,@V_START,121);
		PRINT	@V_DESCRIPTION;
		SET @V_SEQNO		= @V_SEQNO + 1;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

		---------------------------- INSERT INTO TABLE L2_L1_MAPPING_CONFIG --------------------
                SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE L2_L1_MAPPING_CONFIG : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

        TRUNCATE TABLE STAG_ID.STAG_PRE_COVERAGE_ETL5_L2_L1_MAPPING_CONFIG;
        
		INSERT INTO STAG_ID.STAG_PRE_COVERAGE_ETL5_L2_L1_MAPPING_CONFIG (ENTITY_ID, L2, L2_DESC, L1, L1_DESC)
		SELECT
			CASE WHEN ANC_L2.ANALYSIS_CODE LIKE '%SHA%' THEN 'IAS' WHEN ANC_L2.ANALYSIS_CODE LIKE '%CON%' THEN 'IAC' WHEN ANC_L2.ANALYSIS_CODE LIKE '%B71%' THEN 'IAS' END AS ENTITY_ID,
			ANC_L2.ANALYSIS_CODE AS L2,
            ANC_L2.ANALYSIS_NAME AS L2_DESC,
            COALESCE(L1_GHO.LOOK_UP_CD, '0000') AS L1,
            COALESCE(ANC_L1.ANALYSIS_NAME, 'RESERVE') AS L1_DESC
		FROM STAG_ID.STAG_CONFIG_SUNGL_MASTER_ANC_L2 AS ANC_L2
		JOIN STAG_ID.STAG_CONFIG_IFRS17_L1_CODE_GHO_ROLLUP AS L1_GHO ON ANC_L2.ANALYSIS_CODE = L1_GHO.LBU_PARAMETER_CD
		JOIN STAG_ID.STAG_CONFIG_SUNGL_MASTER_ANC_L1 AS ANC_L1 ON L1_GHO.LOOK_UP_CD = ANC_L1.ANALYSIS_CODE;
                

		---------------------------- DROP TEMPORARY TABLE ------------------------------
		IF OBJECT_ID('tempdb..#ETL5_PRE_COVERAGE') IS NOT NULL
		BEGIN
			DROP TABLE #ETL5_PRE_COVERAGE
		END;

		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE ETL5_PRE_COVERAGE : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);		 

		
		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------
--declare @batch varchar(12)='20210301';
--declare @year varchar(12)=substring(@batch,0,5);
--select * from #ETL5_PRE_COVERAGE;
--select * from 			[FOND_ID].[FOND_ETL5_PRE_COVERAGE] where BATCHDATE = '202101';

		with ga as (
			select
				a.*
			from STAG_ID.STAG_LIFEASIA_GA_ALLOWANCE a
			inner join STAG_ID.STAG_CONFIG_STAG_PRE_COVERAGE_ETL5_GA_CONFIG b on a.GA_SALES_CODE_UNIT = b.GA_CODE
			where left(cast(DATE_FROM as varchar), 6) = left(@batch, 6)
		),
		cal as(
			select a.POLICY_ID, b.*
			from (
				select
					a.*, 'ga' as lbl
				from (
					select
						b.HOISSDTE,
						case when substring(cast(HOISSDTE as varchar), 1, 6) = substring(cast(DATE_FROM as varchar), 1, 6) then 1 else 0 end NB_FLAG ,
						a.*
					from ga a
					left join STAG_ID.STAG_LIFEASIA_HPADPF b on a.POLICY_ID = b.CHDRNUM 
				) a where NB_FLAG = 1 
			) a
			left join (
				select *, 'icg' as lbl
				from FOND_ID.FOND_IFRS17_ICG_STORES -- for dev LOCSIT_ICG_STORES_20190131_20201113_TEMP_FOR_DEV
					--for prod change to this STAG_ID.STAG_LOCSIT_ICG_STORE
				where PRODUCT_CD = BENEFIT_CD
			) b on a.POLICY_ID = b.POLICY_NO 
		) ,
			--only for dev:	PRODUCT_CODE = BENEFIT_CODE) b on a.POLICY_ID = b.POLICY_NUMBER ) ,
		fyp as(
			select 
				a.*,PORTFOLIO_GROUP,COHORT_YEAR,SET_OF_CONTRACT,INSURANCE_CONTRACT_GROUP_ID,ICG_ID_PROPHET,ENTRY_MONTH,MEASUREMENT_MODEL 
			from (
				SELECT * 
				from FOND_ID.FOND_ETL5_DRIVER_MASTER 
				where BATCHDATE=left(@batch,6) and POLICY_ALLOC_DRIVER='FYP'
			) a
			inner join (
				select * 
				from FOND_ID.FOND_IFRS17_ICG_STORES 
				where INSURANCE_CONTRACT_GROUP_ID='IAI_PRCONYRTIDR_GMM_'+left(@batch,4)+'_161_GMM'
				and PRODUCT_CD=BENEFIT_CD and INFORCE_FLAG='Y' and STATUS_SOURCE='IF' and ENTRY_MONTH=cast(RIGHT(left(@batch,6),2) as int)
			) b on a.POLICY_NO=b.POLICY_NO 
		) ,
		tot_ as (
			select count(*) COUNT_TOT, COHORT_YEAR
			from cal
			where COHORT_YEAR = @year
			group by COHORT_YEAR
		) ,
--		tot_fyp as (
--		select
--			count(*) COUNT_TOT,
--			COHORT_YEAR
--		from
--			fyp
--		where
--			COHORT_YEAR = @year
--		group by
--			COHORT_YEAR) ,
		icg as (
			select
				lbl, SUB_GROUP_ID, INSURANCE_CONTRACT_GROUP_ID, PRODUCT_CD, PORTFOLIO_GROUP, COHORT_YEAR, ICG_ID_PROPHET, 
				sum(DRIVER_AMT) as SUM_AMT,
				COUNT(POLICY_ID) as COUNT_POL_NO,
				COUNT_TOT
			from
				(
				SELECT
					a.lbl, a.POLICY_ID,
					a.COHORT_YEAR, ICG_ID_PROPHET ,
					PRODUCT_CD , PORTFOLIO_GROUP ,
					a.INSURANCE_CONTRACT_GROUP_ID, 
	--				b.COUNT_TOT as COUNT_TOT,
	--				n.INSURANCE_CONTRACT_GROUP_ID+'_'+upper(convert(char(3),cast(cast(n.COHORT_YEAR as VARCHAR(9))+'-'+cast(n.ENTRY_MONTH as VARCHAR(9))+'-28' as date),0)) SUB_GROUP_ID
					case when a.lbl='ga' then b.COUNT_TOT else null end as COUNT_TOT,
					case when a.lbl='fyp' then a.DRIVER_AMT else null end as DRIVER_AMT,
					case when a.lbl='ga' and n.INSURANCE_CONTRACT_GROUP_ID is not null then n.INSURANCE_CONTRACT_GROUP_ID+'_'+upper(convert(char(3),cast(cast(n.COHORT_YEAR as VARCHAR(9))+'-'+cast(n.ENTRY_MONTH as VARCHAR(9))+'-28' as date),0))
					when a.lbl='ga' and n.INSURANCE_CONTRACT_GROUP_ID is null then a.INSURANCE_CONTRACT_GROUP_ID+'_'+upper(convert(char(3),cast(cast(a.COHORT_YEAR as VARCHAR(9))+'-'+cast(a.ENTRY_MONTH as VARCHAR(9))+'-28' as date),0))
					else a.INSURANCE_CONTRACT_GROUP_ID+'_'+upper(convert(char(3),cast(cast(a.COHORT_YEAR as VARCHAR(9))+'-'+cast(a.ENTRY_MONTH as VARCHAR(9))+'-28' as date),0)) end SUB_GROUP_ID
	--				n.SUB_GROUP_ID
				from (
					select * 
					from (
						select POLICY_ID,COHORT_YEAR,ENTRY_MONTH,ICG_ID_PROPHET,PRODUCT_CD,PORTFOLIO_GROUP,INSURANCE_CONTRACT_GROUP_ID,null DRIVER_AMT,'ga' lbl from cal
						union all
						select POLICY_NO,COHORT_YEAR,ENTRY_MONTH,ICG_ID_PROPHET,PRODUCT_CD,PORTFOLIO_GROUP,INSURANCE_CONTRACT_GROUP_ID,DRIVER_AMT,'fyp' lbl from fyp
					) p where COHORT_YEAR = @year
				) a
				left join tot_ b on a.COHORT_YEAR = b.COHORT_YEAR and a.lbl='ga'
	--			left join tot_fyp d on
	--				a.COHORT_YEAR = d.COHORT_YEAR and a.lbl='fyp'
				left join (
					select
						distinct POLICY_NO POLICY_NUMBER,
						SUB_GROUP_ID,
						INSURANCE_CONTRACT_GROUP_ID,ENTRY_MONTH,COHORT_YEAR
					from FOND_ID.FOND_IFRS17_BENEFIT_INSURANCE_CONTRACT_GROUP_MASTER --for prod change to this
						--only for dev
	--					LOCSIT_ABST_BENEFIT_INSURANCE_GROUP_20190131_20201113_TEMP_FOR_DEV ) n on
				) n on a.POLICY_ID = n.POLICY_NUMBER and a.INSURANCE_CONTRACT_GROUP_ID = n.INSURANCE_CONTRACT_GROUP_ID 
			) d
			group by lbl, INSURANCE_CONTRACT_GROUP_ID, PRODUCT_CD, PORTFOLIO_GROUP, COHORT_YEAR, ICG_ID_PROPHET, COUNT_TOT, SUB_GROUP_ID 
		) ,
		extraction as (
			select 
				a.*,
				cast(COUNT_POL_NO as numeric(28, 10))/ cast(COUNT_TOT as numeric(28, 10))* 100 as WEIGHTED_VALUE
			from (
				select distinct *
				from icg
			) a where SUB_GROUP_ID is not null 
		),
		put_default_val as (
			select
				'IAI' ENTITY_ID,
				null REPORTING_DT,
				'N' CEDED_FLAG,
				'GL_ACCOUNT' ALLOCATION_MEASUREMENT_TYPE_CD,
				'IDR' CURRENCY_CD,
				0 PCA_DIRECT_ALLOC_AMT,
				0 IMPAIR_LOSS_INPUT_AMT,
				0 IMPAIR_LOSS_RCOERY_INPUT_AMT,
				'Y' DERECOGNITION_FLAG,
				case when lbl='ga' then 'CAT1_' + SUBSTRING(@batch, 3, 2)+ 'Q' + cast(DATEPART(QUARTER, @batch) as varchar)
				else 'CAT2_' + SUBSTRING(@batch, 3, 2)+ 'Q' + cast(DATEPART(QUARTER, @batch) as varchar)
				end ALLOCATION_SEGMENT_ID,
				*
			from extraction 
		),
		add_segment_cashflow as (
			select
				a.*,
				b.L1 SEGMENT_L1,
				c.CASHFLOW_TYPE_L2 MEASUREMENT_VAR_NM
			from put_default_val a
			left join STAG_ID.STAG_PRE_COVERAGE_ETL5_L2_L1_MAPPING_CONFIG b on a.PORTFOLIO_GROUP = b.L2
			left join (
				select DISTINCT ALLOCATION_SEGMENT, CASHFLOW_TYPE_L2
				from STAG_ID.STAG_CONFIG_STAG_PRE_COVERAGE_ETL5_COMPENSATION_SCHEME_CONFIG
				WHERE ENTITY_ID = 'IAI'
			) c on a.ALLOCATION_SEGMENT_ID = c.ALLOCATION_SEGMENT
		) ,
		de_recognition_step1 as (
			select
				'key' joinkey,
				a.*,
				b.DE_RECOGNITION_PATTERN_PER_MONTH DRPPM_LI,
				c.DE_RECOGNITION_PATTERN_PER_MONTH DRPPM_PF,
				d.DE_RECOGNITION_PATTERN_PER_MONTH DRPPM_GD,
				b.PROPORTION PROPORTION_LI,
				c.PROPORTION PROPORTION_PF,
				d.PROPORTION PROPORTION_GD,
				cast(ROUND(COUNT_POL_NO*b.PROPORTION,0,0) as int) as LEADER_INCENTIVE,
				cast(ROUND(COUNT_POL_NO*c.PROPORTION,0,0) as int) as PERSONAL_FEE,
				cast(ROUND(COUNT_POL_NO*d.PROPORTION,0,0) as int) as GA_DEVELOPMENT
			from (
				select *, 'key' as join_key
				from extraction where lbl='ga' 
			) a
			left join (
				select
					COMPENSATION_SCHEME,
					EXPENSE_CATEGORY,
					cast(REPLACE([DE_RECOGNITION_PATTERN_PER_MONTH ], '%', '') as float)/ 100 as DE_RECOGNITION_PATTERN_PER_MONTH ,
					ALLOCATION_SEGMENT,
					CASHFLOW_TYPE_L2,
					TERM,
					PREPAID_AMOUNT,
					ANNUAL_AMOUNT,
					ANNUAL_PERIOD,
					cast(REPLACE(PROPORTION, '%', '') as float)/ 100 as PROPORTION ,
					'key' as join_key
				from STAG_ID.STAG_CONFIG_STAG_PRE_COVERAGE_ETL5_COMPENSATION_SCHEME_CONFIG
				where ALLOCATION_SEGMENT = 'CAT1_' + Substring(@batch, 3, 2) + 'Q' + Cast(Datepart(quarter, @batch) AS VARCHAR) and COMPENSATION_SCHEME like 'Leader Incentiv%' 
				AND ENTITY_ID = 'IAI'
			) b on a.join_key = b.join_key
			left join (
				select
					COMPENSATION_SCHEME,
					EXPENSE_CATEGORY,
					cast(REPLACE([DE_RECOGNITION_PATTERN_PER_MONTH ], '%', '') as float)/ 100 as DE_RECOGNITION_PATTERN_PER_MONTH ,
					ALLOCATION_SEGMENT,
					CASHFLOW_TYPE_L2 ,
					TERM ,
					PREPAID_AMOUNT ,
					ANNUAL_AMOUNT,
					ANNUAL_PERIOD,
					cast(REPLACE(PROPORTION, '%', '') as float)/ 100 as PROPORTION ,
					'key' as join_key
				from STAG_ID.STAG_CONFIG_STAG_PRE_COVERAGE_ETL5_COMPENSATION_SCHEME_CONFIG
				where ALLOCATION_SEGMENT = 'CAT1_' + Substring(@batch, 3, 2) + 'Q' + Cast(Datepart(quarter, @batch) AS VARCHAR) and COMPENSATION_SCHEME like 'Personal Fe%' 
				AND ENTITY_ID = 'IAI'
			) c on a.join_key = c.join_key
			left join (
				select
					COMPENSATION_SCHEME,
					EXPENSE_CATEGORY,
					cast(REPLACE([DE_RECOGNITION_PATTERN_PER_MONTH ], '%', '') as float)/ 100 as DE_RECOGNITION_PATTERN_PER_MONTH ,
					ALLOCATION_SEGMENT,
					CASHFLOW_TYPE_L2 ,
					TERM ,
					PREPAID_AMOUNT ,
					ANNUAL_AMOUNT,
					ANNUAL_PERIOD,
					cast(REPLACE(PROPORTION, '%', '') as float)/ 100 as PROPORTION ,
					'key' as join_key
				from STAG_ID.STAG_CONFIG_STAG_PRE_COVERAGE_ETL5_COMPENSATION_SCHEME_CONFIG
				where ALLOCATION_SEGMENT = 'CAT1_' + Substring(@batch, 3, 2) + 'Q' + Cast(Datepart(quarter, @batch) AS VARCHAR) and COMPENSATION_SCHEME like 'GA Developmen%' 
				AND ENTITY_ID = 'IAI'
			) d on a.join_key = d.join_key
		),
		de_recognition_step1_cat2 as (
			select
				'key' joinkey,
				a.*,
				b.DE_RECOGNITION_PATTERN_PER_MONTH DRPPM_SRE,
				c.DE_RECOGNITION_PATTERN_PER_MONTH DRPPM_SDC,			
				b.PROPORTION PROPORTION_SRE,
				c.PROPORTION PROPORTION_SDC,
				cast(ROUND(SUM_AMT*b.PROPORTION,0,0) as int) as SRE,
				cast(ROUND(SUM_AMT*c.PROPORTION,0,0) as int) as SDC		
			from (
				select *, 'key' as join_key
				from extraction where lbl='fyp'
			) a
			left join (
				select
					COMPENSATION_SCHEME,
					EXPENSE_CATEGORY,
					cast(REPLACE([DE_RECOGNITION_PATTERN_PER_MONTH ], '%', '') as float)/ 100 as DE_RECOGNITION_PATTERN_PER_MONTH ,
					replace(ALLOCATION_SEGMENT,'CAT1','CAT2') ALLOCATION_SEGMENT,
					[CASHFLOW_TYPE_L2 ],
					[TERM ],
					[PREPAID_AMOUNT ],
					ANNUAL_AMOUNT,
					ANNUAL_PERIOD,
					cast(REPLACE(PROPORTION, '%', '') as float)/ 100 as PROPORTION ,
					'key' as join_key
				from STAG_ID.STAG_CONFIG_STAG_PRE_COVERAGE_ETL5_COMPENSATION_SCHEME_CONFIG
				where ALLOCATION_SEGMENT = 'CAT2_' + Substring(@batch, 3, 2) + 'Q' + Cast(Datepart(quarter, @batch) AS VARCHAR) and COMPENSATION_SCHEME like 'SRE%' 
				AND ENTITY_ID = 'IAI'
			) b on a.join_key = b.join_key
			left join (
				select
					COMPENSATION_SCHEME,
					EXPENSE_CATEGORY,
					cast(REPLACE([DE_RECOGNITION_PATTERN_PER_MONTH ], '%', '') as float)/ 100 as DE_RECOGNITION_PATTERN_PER_MONTH ,
					ALLOCATION_SEGMENT,
					CASHFLOW_TYPE_L2,
					TERM,
					PREPAID_AMOUNT,
					ANNUAL_AMOUNT,
					ANNUAL_PERIOD,
					cast(REPLACE(PROPORTION, '%', '') as float)/ 100 as PROPORTION ,
					'key' as join_key
				from STAG_ID.STAG_CONFIG_STAG_PRE_COVERAGE_ETL5_COMPENSATION_SCHEME_CONFIG
				where ALLOCATION_SEGMENT = 'CAT2_' + Substring(@batch, 3, 2) + 'Q' + Cast(Datepart(quarter, @batch) AS VARCHAR) and COMPENSATION_SCHEME like 'SDC%' 
				AND ENTITY_ID = 'IAI'
			) c on a.join_key = c.join_key
		 ) ,
		de_recognition_step2 as (
			SELECT
				'key' joinkey,
				COHORT_YEAR,
				sum(LEADER_INCENTIVE) as SUM_LI,
				sum(PERSONAL_FEE) as SUM_PF,
				sum(GA_DEVELOPMENT) as SUM_GD
			from de_recognition_step1
			group by COHORT_YEAR 
		),
		de_recognition_step2_cat2 as (
			SELECT
				'key' joinkey,
				COHORT_YEAR,
				sum(SRE) as SUM_SRE,
				sum(SDC) as SUM_SDC
			from de_recognition_step1_cat2
			group by COHORT_YEAR 
		),
		de_recognition_step3 as (
			select 
				*,
				PERCENT_LI + PERCENT_PF + PERCENT_GD as DE_RECOGNITION
			from (
				select
					a.SUB_GROUP_ID,
					a.DRPPM_LI,
					a.DRPPM_PF,
					a.DRPPM_GD,
					b.SUM_LI,
					b.SUM_PF,
					b.SUM_GD,
					a.LEADER_INCENTIVE,
					a.PERSONAL_FEE,
					a.GA_DEVELOPMENT,
					COALESCE(a.DRPPM_LI / NULLIF(cast(b.SUM_LI as float)*cast(a.LEADER_INCENTIVE as float),0),0) as PERCENT_LI,
					COALESCE(a.DRPPM_PF / NULLIF(cast(b.SUM_PF as float)*cast(a.PERSONAL_FEE as float),0),0) as PERCENT_PF,
					COALESCE(a.DRPPM_GD / NULLIF(cast(b.SUM_GD as float)*cast(a.GA_DEVELOPMENT as float),0),0) as PERCENT_GD
				from de_recognition_step1 a
				left join de_recognition_step2 b on a.joinkey = b.joinkey 
			) a 
		),
		de_recognition_step3_cat2 as (
			select
				*,
				PERCENT_SRE + PERCENT_SDC as DE_RECOGNITION
			from (
				select
					a.SUB_GROUP_ID,
					a.DRPPM_SRE,
					a.DRPPM_SDC,
					b.SUM_SRE,
					b.SUM_SDC,
					a.SRE,
					a.SDC,
	--				COALESCE(a.DRPPM_SRE / NULLIF(cast(b.SUM_SRE as float)*cast(a.SRE as float),0),0) as PERCENT_SRE,
	--				COALESCE(a.DRPPM_SDC / NULLIF(cast(b.SUM_SDC as float)*cast(a.SDC as float),0),0) as PERCENT_SDC
					COALESCE(a.DRPPM_SRE,0) as PERCENT_SRE,
					COALESCE(a.DRPPM_SDC,0) as PERCENT_SDC
				from de_recognition_step1_cat2 a
				left join de_recognition_step2_cat2 b on a.joinkey = b.joinkey 
			) a 
		) 
	select * 
	into #ETL5_PRE_COVERAGE 
	from (
		SELECT 
			ENTITY_ID, 
			null REPORTING_DT, 
			CEDED_FLAG, 
			ALLOCATION_MEASUREMENT_TYPE_CD, 
			MEASUREMENT_VAR_NM, 
			cast(a.SUB_GROUP_ID as varchar(100))   AS TARGET_GROUP_ID,  
			PORTFOLIO_GROUP+'_'+ALLOCATION_SEGMENT_ID as ALLOCATION_SEGMENT_ID, 
			DERECOGNITION_FLAG, 
			SEGMENT_L1, 
			COALESCE(WEIGHTED_VALUE/100,0) AS ALLOCATION_WEIGHT, 
			CURRENCY_CD, 
			PCA_DIRECT_ALLOC_AMT, 
			b.DE_RECOGNITION/100 as PCA_ALLOC_REP_INT_PCT,
			IMPAIR_LOSS_INPUT_AMT, 
			IMPAIR_LOSS_RCOERY_INPUT_AMT 
		FROM  add_segment_cashflow a
		left join  (
		  select SUB_GROUP_ID,DE_RECOGNITION from de_recognition_step3 
		  union all
		  select SUB_GROUP_ID,DE_RECOGNITION from de_recognition_step3_cat2
		) b on a.SUB_GROUP_ID=b.SUB_GROUP_ID
		WHERE NOT(SUBSTRING(a.ALLOCATION_SEGMENT_ID, 1,4) = 'CAT2' AND CAST(SUBSTRING(a.ALLOCATION_SEGMENT_ID, 6,2) AS INTEGER) >= 22)
	) H

	---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.[FOND_ETL5_PRE_COVERAGE] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	 

		DELETE FROM FOND_ID.FOND_ETL5_PRE_COVERAGE 
		WHERE BATCHDATE = left(@batch,6) AND ENTITY_ID = 'IAI';

		 ---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_PRE_COVERAGE] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

		INSERT INTO [FOND_ID].[FOND_ETL5_PRE_COVERAGE]
			(ENTITY_ID
			,RPT_DT
			,CEDED_FLG
			,ALLOC_MEASURE_TYPE_CD
			,MEASURE_VAR_NAME
			,TGT_GRP_ID
			,ALLOC_SEGMENT_ID
			,DERECOG_FLG
			,L1_SEG_FUND
			,ALLOC_WGHTD_AVG
			,CCY_CD
			,PCA_DIRECT_ALLOC_AMT
			,PCA_ALLOC_REP_INT_PCT
			,IMPAIR_LOSS_RCOV_INPUT_AMT
			,IMPAIR_LOSS_INPUT_AMT
			,BATCH_MASTER_ID,
			BATCH_RUN_ID,
			JOB_MASTER_ID,
			JOB_RUN_ID,
			BATCHDATE,
			ETL_PROCESS_DATE_TIME)
		SELECT
			ENTITY_ID,
			REPORTING_DT,
			CEDED_FLAG,
			ALLOCATION_MEASUREMENT_TYPE_CD,
			MEASUREMENT_VAR_NM,
			TARGET_GROUP_ID,
			ALLOCATION_SEGMENT_ID,
			DERECOGNITION_FLAG,
			SEGMENT_L1,
			ALLOCATION_WEIGHT,
			CURRENCY_CD,
			PCA_DIRECT_ALLOC_AMT,
			PCA_ALLOC_REP_INT_PCT,
			IMPAIR_LOSS_RCOERY_INPUT_AMT,
			IMPAIR_LOSS_INPUT_AMT,
			@BATCH_MASTER_ID BATCH_MASTER_ID,
			@BATCH_RUN_ID BATCH_RUN_ID,
			@JOB_MASTER_ID JOB_MASTER_ID,
			@JOB_RUN_ID JOB_RUN_ID,
			left(@batch,6) BATCHDATE,
			GETDATE() ETL_PROCESS_DATE_TIME
		FROM #ETL5_PRE_COVERAGE
		;

		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #ETL5_PRE_COVERAGE) ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_PRE_COVERAGE'
		,@drivername,@V_TOTAL_ROWS,'YTD',@V_PERIOD);
		
		
		IF @@TRANCOUNT > 0
        COMMIT;


		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#ETL5_PRE_COVERAGE') IS NOT NULL
		BEGIN
			DROP TABLE #ETL5_PRE_COVERAGE
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