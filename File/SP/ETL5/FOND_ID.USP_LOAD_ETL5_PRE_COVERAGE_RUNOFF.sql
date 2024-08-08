CREATE PROC [FOND_ID].[USP_LOAD_ETL5_PRE_COVERAGE_RUNOFF] @batch [NVARCHAR](100),@JOBNAMESTR [NVARCHAR](2000) AS
BEGIN 
	--DECLARE @batch [nvarchar](30);
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.USP_LOAD_ETL5_PRE_COVERAGE_RUNOFF';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	DECLARE @drivername NVARCHAR(15);
	SET @drivername = 'PRE_COVERAGE_RUNOFF';

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
	
		---------------------------- DROP TEMPORARY TABLE ------------------------------
		IF OBJECT_ID('tempdb..#ETL5_PRE_COVERAGE_RUNOFF') IS NOT NULL
		BEGIN
			DROP TABLE #ETL5_PRE_COVERAGE_RUNOFF
		END;

		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'SELECT INTO TEMPORARY TABLE ETL5_PRE_COVERAGE : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);		 


		---------------------------- INSERT INTO TEMPORARY TABLE ------------------------------

		IF OBJECT_ID('tempdb..#get_rem_year') IS NOT NULL
		BEGIN
			DROP TABLE #get_rem_year
		END;
		IF OBJECT_ID('tempdb..#get_rem_yearcat2') IS NOT NULL
		BEGIN
			DROP TABLE #get_rem_yearcat2
		END;
		IF OBJECT_ID('tempdb..#tmp_proj_year_pf') IS NOT NULL
		BEGIN
			DROP TABLE #tmp_proj_year_pf
		END;
		IF OBJECT_ID('tempdb..#tmp_proj_year_gd') IS NOT NULL
		BEGIN
			DROP TABLE #tmp_proj_year_gd
		END;
		IF OBJECT_ID('tempdb..#tmp_proj_year_li') IS NOT NULL
		BEGIN
			DROP TABLE #tmp_proj_year_li
		END;
			IF OBJECT_ID('tempdb..#tmp_proj_year_sre') IS NOT NULL
		BEGIN
			DROP TABLE #tmp_proj_year_sre
		END;
			IF OBJECT_ID('tempdb..#tmp_proj_year_sdc') IS NOT NULL
		BEGIN
			DROP TABLE #tmp_proj_year_sdc
		END;
		IF OBJECT_ID('tempdb..#tmp_proj_year') IS NOT NULL
		BEGIN
			DROP TABLE #tmp_proj_year
		END;
		IF OBJECT_ID('tempdb..#final_runoff') IS NOT NULL
		BEGIN
			DROP TABLE #final_runoff
		END;
		IF OBJECT_ID('tempdb..#ETL5_PRE_COVERAGE_RUNOFF') IS NOT NULL
		BEGIN
			DROP TABLE #ETL5_PRE_COVERAGE_RUNOFF
		END;
		
--declare @batch varchar(12)='20210101';
		
		declare @batchdate varchar(100)=@batch
		declare @confperiod varchar(15)='CAT1_' + SUBSTRING(@batchdate, 3, 2)+ 'Q' + cast(DATEPART(QUARTER, @batchdate) as varchar)
		declare @confperiod2 varchar(15)='CAT2_' + SUBSTRING(@batchdate, 3, 2)+ 'Q' + cast(DATEPART(QUARTER, @batchdate) as varchar)
		--		declare @dateperiod date= EOMONTH(convert(date,@batchdate,112));
		declare @dateperiod date=DATEADD(dd, -1, DATEADD(qq, DATEDIFF(qq, 0, convert(date,@batchdate,112)) +1, 0))
		------------------------CAT1
		--drop table #get_rem_year
		select * into #get_rem_year from (
		select *,cast(remaining_month as float)/12 as remaining_year from (
		select *,case when TERM-recognition_period <0 then 0 else TERM-recognition_period end as remaining_month from (
		select *,datediff(month,START_DATE,@dateperiod) as recognition_period from STAG_ID.STAG_CONFIG_STAG_PRE_COVERAGE_ETL5_COMPENSATION_SCHEME_CONFIG where ALLOCATION_SEGMENT=@confperiod and ENTITY_ID = 'IAI'
		) a ) d
		) n
		------------------------CAT2
		--drop table #get_rem_yearcat2
		select * into #get_rem_yearcat2 from (
		select *,cast(remaining_month as float)/12 as remaining_year from (
		select *,case when TERM-recognition_period <0 then 0 else TERM-recognition_period end as remaining_month from (
		select *,datediff(month,START_DATE,@dateperiod) as recognition_period from STAG_ID.STAG_CONFIG_STAG_PRE_COVERAGE_ETL5_COMPENSATION_SCHEME_CONFIG where ALLOCATION_SEGMENT=@confperiod2 and ENTITY_ID = 'IAI'
		) a ) d
		) n
		--select *  from #get_rem_yearcat2
		-----------------------------Personal Fee-----------------------------
		--drop table #tmp_proj_year_pf
		create table #tmp_proj_year_pf (projection_year int,year float,risk_driver_amt numeric(28,2))
		declare @max_ float=(select remaining_year from #get_rem_year where COMPENSATION_SCHEME='Personal Fee');
		declare @prop numeric(28,2)=(select case when DERECOGNITION_PROPORTION is null or DERECOGNITION_PROPORTION ='' then 0 else cast(replace(DERECOGNITION_PROPORTION,'%','') as numeric(28,2))/100 end from #get_rem_year where COMPENSATION_SCHEME='Personal Fee');
		declare @max_round int=ceiling(@max_)
		declare @max_round_ori float=@max_
		declare @i int=1;
		--select  @i_round
		while @i <= @max_round
		BEGIN
			if @i<@max_round
				insert into #tmp_proj_year_pf select @i as projection_year,1 as year,(1/@max_)*@prop as risk_driver_amt
			else if @i=@max_round
				insert into #tmp_proj_year_pf select @i as projection_year, @max_round_ori-(@i-1) as year,((@max_round_ori-(@i-1))/@max_)*@prop as risk_driver_amt		
			set @i +=1;
		END
		--select * from  #tmp_proj_year_pf
		
		-----------------------------GA Development-----------------------------
		--drop table #tmp_proj_year_gd
		create table #tmp_proj_year_gd (projection_year int,year float,risk_driver_amt numeric(28,2))
		declare @max_1 float=(select remaining_year from #get_rem_year where COMPENSATION_SCHEME='GA Development');
		declare @prop1 numeric(28,2)=(select case when DERECOGNITION_PROPORTION is null or DERECOGNITION_PROPORTION ='' then 0 else cast(replace(DERECOGNITION_PROPORTION,'%','') as numeric(28,2))/100 end from #get_rem_year where COMPENSATION_SCHEME='GA Development');
		declare @max_round1 int=ceiling(@max_1)
		declare @max_round_ori1 float=@max_1
		declare @i1 int=1;
		--select  @i_round
		while @i1 <= @max_round1
		BEGIN
			if @i1<@max_round1
				insert into #tmp_proj_year_gd select @i1 as projection_year,1 as year,(1/@max_1)*@prop1 as risk_driver_amt
			else if @i1=@max_round1
				insert into #tmp_proj_year_gd select @i1 as projection_year, @max_round_ori1-(@i1-1) as year,((@max_round_ori1-(@i1-1))/@max_1)*@prop1 as risk_driver_amt		
			set @i1 +=1;
		END
		
		-----------------------------Leader Incentive-----------------------------
		--drop table #tmp_proj_year_li
		create table #tmp_proj_year_li (projection_year int,year float,risk_driver_amt numeric(28,2))
		declare @max_2 float=(select remaining_year from #get_rem_year where COMPENSATION_SCHEME='Leader Incentive');
		declare @prop2 numeric(28,2)=(select case when DERECOGNITION_PROPORTION is null or DERECOGNITION_PROPORTION ='' then 0 else cast(replace(DERECOGNITION_PROPORTION,'%','') as numeric(28,2))/100 end from #get_rem_year where COMPENSATION_SCHEME='Leader Incentive');
		declare @max_round2 int=ceiling(@max_2)
		declare @max_round_ori2 float=@max_2
		declare @i2 int=1;
		--select  @i_round
		while @i2 <= @max_round2
		BEGIN
			if @i2<@max_round2
				insert into #tmp_proj_year_li select @i2 as projection_year,1 as year,(1/@max_2)*@prop2 as risk_driver_amt
			else if @i2=@max_round2
				insert into #tmp_proj_year_li select @i2 as projection_year, @max_round_ori2-(@i2-1) as year,((@max_round_ori2-(@i2-1))/@max_2)*@prop2 as risk_driver_amt		
			set @i2 +=1;
		END
--		select * from #tmp_proj_year_li
		-----------------------------SRE-----------------------------
		--drop table #tmp_proj_year_sre
		create table #tmp_proj_year_sre (projection_year int,year float,risk_driver_amt numeric(28,2))
		declare @max_3 float=(select remaining_year from #get_rem_yearcat2 where COMPENSATION_SCHEME like 'SRE%');
		declare @prop3 numeric(28,2)=(select case when DERECOGNITION_PROPORTION is null or DERECOGNITION_PROPORTION ='' then 0 else cast(replace(DERECOGNITION_PROPORTION,'%','') as numeric(28,2))/100 end from #get_rem_yearcat2 where COMPENSATION_SCHEME like 'SRE%');
		declare @max_round3 int=ceiling(@max_3)
		declare @max_round_ori3 float=@max_3
		declare @i3 int=1;
		--select  @i_round
		while @i3 <= @max_round3
		BEGIN
			if @i3<@max_round3
				insert into #tmp_proj_year_sre select @i3 as projection_year,1 as year,(1/@max_3)*@prop3 as risk_driver_amt
			else if @i3=@max_round3
				insert into #tmp_proj_year_sre select @i3 as projection_year, @max_round_ori3-(@i3-1) as year,((@max_round_ori3-(@i3-1))/@max_3)*@prop3 as risk_driver_amt		
			set @i3 +=1;
		END
--		select * from #tmp_proj_year_sre
		-----------------------------SDC-----------------------------
		--drop table #tmp_proj_year_sdc
		create table #tmp_proj_year_sdc (projection_year int,year float,risk_driver_amt numeric(28,2))
		declare @max_4 float=(select remaining_year from #get_rem_yearcat2 where COMPENSATION_SCHEME like 'SDC%');
		declare @prop4 numeric(28,2)=(select case when DERECOGNITION_PROPORTION is null or DERECOGNITION_PROPORTION ='' then 0 else cast(replace(DERECOGNITION_PROPORTION,'%','') as numeric(28,2))/100 end from #get_rem_yearcat2 where COMPENSATION_SCHEME like 'SDC%');
		declare @max_round4 int=ceiling(@max_4)
		declare @max_round_ori4 float=@max_4
		declare @i4 int=1;
		--select  @i_round
		while @i4 <= @max_round4
		BEGIN
			if @i4<@max_round4
				insert into #tmp_proj_year_sdc select @i4 as projection_year,1 as year,(1/@max_4)*@prop4 as risk_driver_amt
			else if @i4=@max_round4
				insert into #tmp_proj_year_sdc select @i4 as projection_year, @max_round_ori4-(@i4-1) as year,((@max_round_ori4-(@i4-1))/@max_4)*@prop4 as risk_driver_amt		
			set @i4 +=1;
		END
--		select * from #tmp_proj_year_sdc
		
		-----------------------------Calculate RISK_DRIVER_AMT-----------------------------
		--drop table #tmp_proj_year
		select *,RISK_DRIVER_AMT_PF+RISK_DRIVER_AMT_LI+RISK_DRIVER_AMT_GD as RISK_DRIVER_AMT_TOT,
		RISK_DRIVER_AMT_SRE+RISK_DRIVER_AMT_SDC as RISK_DRIVER_AMT_TOT_CAT2 
		into #tmp_proj_year 
		from 
		( select n.PY_LI,n.PY_PF,n.PY_GD,n.PY_SRE,case when isnull(e.projection_year,0)=0  then n.PY_LI else e.projection_year end as PY_SDC,
		n.Y_LI,n.Y_PF,n.Y_GD,n.Y_SRE,isnull(e.year,0) as Y_SDC,
		n.RISK_DRIVER_AMT_LI,n.RISK_DRIVER_AMT_PF,n.RISK_DRIVER_AMT_GD,n.RISK_DRIVER_AMT_SRE,isnull(e.risk_driver_amt,0) as RISK_DRIVER_AMT_SDC
		from
		( select n.PY_LI,n.PY_PF,n.PY_GD,case when isnull(d.projection_year,0)=0  then n.PY_LI else d.projection_year end as PY_SRE,
		n.Y_LI,n.Y_PF,n.Y_GD,isnull(d.year,0) as Y_SRE,
		n.RISK_DRIVER_AMT_LI,n.RISK_DRIVER_AMT_PF,n.RISK_DRIVER_AMT_GD,isnull(d.risk_driver_amt,0) as RISK_DRIVER_AMT_SRE
		from
		( select n.PY_LI,n.PY_PF,case when isnull(c.projection_year,0)=0  then n.PY_LI else c.projection_year end as PY_GD,
		n.Y_LI,n.Y_PF,isnull(c.year,0) as Y_GD,
		n.RISK_DRIVER_AMT_LI,n.RISK_DRIVER_AMT_PF,isnull(c.risk_driver_amt,0) as RISK_DRIVER_AMT_GD
		from
		(select case when isnull(a.projection_year,0)=0 then b.projection_year else a.projection_year end as PY_LI,
		case when isnull(b.projection_year,0)=0 then a.projection_year else b.projection_year end as PY_PF,
		isnull(a.year,0) as Y_LI,isnull(b.year,0) as Y_PF,
		isnull(a.risk_driver_amt,0) as RISK_DRIVER_AMT_LI,isnull(b.risk_driver_amt,0) as RISK_DRIVER_AMT_PF
		from #tmp_proj_year_li a
		full outer join
		(select *  from #tmp_proj_year_pf) b on a.projection_year=b.projection_year ) n
		full outer join
		(select * from #tmp_proj_year_gd) c on n.PY_LI=c.projection_year or n.PY_PF=c.projection_year
		) n
		full outer join
		(select * from #tmp_proj_year_sre) d on n.PY_LI=d.projection_year or n.PY_PF=d.projection_year or n.PY_GD=d.projection_year
		) n
		full outer join
		(select * from #tmp_proj_year_sdc) e on n.PY_LI=e.projection_year or n.PY_PF=e.projection_year or n.PY_GD=e.projection_year or n.PY_SRE=e.projection_year
		) n
--		select * from #tmp_proj_year
		-----------------------------Finalize Output-----------------------------
--declare @batchdate varchar(100)='20210101'
		select * 
		into #ETL5_PRE_COVERAGE_RUNOFF 
		from (
		select 'IAI' ENTITY_ID,null as REPORTING_DT,
		'CAT1_' + SUBSTRING(@batchdate, 3, 2)+ 'Q' + cast(DATEPART(QUARTER, @batchdate) as varchar) INSURANCE_SEGMENT_ID,
		 PY_LI as PROJECTION_YEAR,RISK_DRIVER_AMT_TOT as RISK_DRIVER_AMT
		from #tmp_proj_year where RISK_DRIVER_AMT_TOT>0
		union all
		select 'IAI' ENTITY_ID,null as REPORTING_DT,
		'CAT2_' + SUBSTRING(@batchdate, 3, 2)+ 'Q' + cast(DATEPART(QUARTER, @batchdate) as varchar) INSURANCE_SEGMENT_ID,
		 PY_LI as PROJECTION_YEAR,RISK_DRIVER_AMT_TOT_CAT2 as RISK_DRIVER_AMT
		from #tmp_proj_year where RISK_DRIVER_AMT_TOT_CAT2>0
		) l
--		select distinct a.ENTITY_ID,a.REPORTING_DT,a.PROJECTION_YEAR,a.RISK_DRIVER_AMT,b.ALLOC_SEGMENT_ID as INSURANCE_SEGMENT_ID from #ETL5_PRE_COVERAGE_RUNOFF a left  join FOND_ID.FOND_ETL5_PRE_COVERAGE b on a.INSURANCE_SEGMENT_ID=right(b.ALLOC_SEGMENT_ID,9) order by ALLOC_SEGMENT_ID


---------------------------- TO Handle rerun process ------------------------------
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'DELETE DATA FROM FOND_ID.[FOND_ETL5_PRE_COVERAGE_RUNOFF] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);	 

		DELETE FROM FOND_ID.FOND_ETL5_PRE_COVERAGE_RUNOFF 
		WHERE BATCHDATE = left(@batch,6) and ENTITY_ID = 'IAI';

		 ---------------------------- TO Handle rerun process ------------------------------
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'INSERT INTO TABLE FOND_ID.[FOND_ETL5_PRE_COVERAGE_RUNOFF] : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);

--declare @batch varchar(100)='20210101'
		INSERT
			INTO
			FOND_ID.FOND_ETL5_PRE_COVERAGE_RUNOFF (ENTITY_ID, REPORTING_DT, INSURANCE_SEGMENT_ID, PROJECTION_YEAR, RISK_DRIVER_AMT, BATCH_MASTER_ID, BATCH_RUN_ID, JOB_MASTER_ID, JOB_RUN_ID, BATCHDATE, ETL_PROCESS_DATE_TIME)
		SELECT distinct
			a.ENTITY_ID,
			null REPORTING_DT,
			b.ALLOC_SEGMENT_ID as INSURANCE_SEGMENT_ID,
			a.PROJECTION_YEAR,
			a.RISK_DRIVER_AMT,
			@BATCH_MASTER_ID BATCH_MASTER_ID,
			@BATCH_RUN_ID BATCH_RUN_ID,
			@JOB_MASTER_ID JOB_MASTER_ID,
			@JOB_RUN_ID JOB_RUN_ID,
			left(@batch,6) BATCHDATE,
			GETDATE() ETL_PROCESS_DATE_TIME
		FROM
			#ETL5_PRE_COVERAGE_RUNOFF a
		left  join FOND_ID.FOND_ETL5_PRE_COVERAGE b on a.INSURANCE_SEGMENT_ID=right(b.ALLOC_SEGMENT_ID,9) AND a.ENTITY_ID = b.ENTITY_ID
		;
		---------------------------- ETL5 LOGGING ----------------------------      
       	
		DECLARE @V_TOTAL_ROWS integer = 0;
		DECLARE @V_PERIOD nvarchar(10);
		SET @V_TOTAL_ROWS = (SELECT COUNT(1) as totalrows FROM #ETL5_PRE_COVERAGE_RUNOFF) ;
        SET @V_PERIOD = CONCAT(YEAR(DATEADD(month, 0,CONVERT(date, @batch))), RIGHT(CONCAT('000', MONTH(DATEADD(month, 0,CONVERT(date, @batch)))),3))

		INSERT INTO FOND_ID.FOND_IFRS17_ETL5_PROC_LOG (PROC_DATE,FUNC_NAME,TRGT_TABLE_NAME,DRIVER_NAME,TOTAL_ROWS,DESCRIPTION,PERIOD)
		VALUES (@V_START,@V_FUNCTION_NAME,'FOND_ID.FOND_ETL5_PRE_COVERAGE_RUNOFF'
		,@drivername,@V_TOTAL_ROWS,'',@V_PERIOD);
		
		
		IF @@TRANCOUNT > 0
        COMMIT;


		---------------------------- DROP TEMPORARY TABLE ------------------------------  
		IF OBJECT_ID('tempdb..#ETL5_PRE_COVERAGE_RUNOFF') IS NOT NULL
		BEGIN
			DROP TABLE #ETL5_PRE_COVERAGE_RUNOFF
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

