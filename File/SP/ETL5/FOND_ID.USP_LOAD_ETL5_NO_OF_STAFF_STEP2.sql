CREATE PROC [FOND_ID].[USP_LOAD_ETL5_NO_OF_STAFF_STEP2] @batchdate [varchar](12),@NB_OUT [varchar](100) OUT,@EB_OUT [varchar](100) OUT AS
BEGIN
	--PRINT	'Start : '+ cast(FORMAT(GETDATE(),'yyyy-MM-dd HH:mm') as varchar(30));
--	declare @batchdate varchar(10)='201911'
	
	-------------------------|DROP TEMP TABLE NEW BUSINESS|---------------------------
	    IF OBJECT_ID('tempdb..#stag_etl5_temp_total_policy_per_channel') IS NOT NULL
		BEGIN
			DROP TABLE #stag_etl5_temp_total_policy_per_channel
		END;
		IF OBJECT_ID('tempdb..#etl5_temp_lifeasia_newbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_lifeasia_newbusiness
		END;
			 	IF OBJECT_ID('tempdb..#etl5_temp_pruaman_newbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_pruaman_newbusiness
		END;
		
		 	IF OBJECT_ID('tempdb..#etl5_temp_pruamans_newbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_pruamans_newbusiness
		END;
		
		 IF OBJECT_ID('tempdb..#etl5_temp_paylife_newbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_paylife_newbusiness
		END;
	
		 IF OBJECT_ID('tempdb..#etl5_temp_creditshield_newbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_creditshield_newbusiness
		END;
	
		 IF OBJECT_ID('tempdb..#etl5_temp_pruemas_newbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_pruemas_newbusiness
		END;
	
--	DECLARE  @V_DRIVER_PERIOD VARCHAR(10) = '2019011'--CONCAT(left(cast(@batchdate as varchar(10)),4),'0',RIGHT(cast(@batchdate as varchar(10)),2));
--	DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='';
--	DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='';
--	DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='';
--	set @V_DRIVER_PERIOD_YEAR = left(@V_DRIVER_PERIOD,4);
--	set @V_DRIVER_PERIOD_MONTH = right(@V_DRIVER_PERIOD,2);
--	set @V_DRIVER_PERIOD_FIRST_MONTH = CONCAT(@V_DRIVER_PERIOD,'01');
	
	DECLARE  @V_DRIVER_PERIOD VARCHAR(10) =@batchdate;
	DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='';
	DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='';
	DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH date ='';
	set @V_DRIVER_PERIOD_YEAR = left(cast(@V_DRIVER_PERIOD as varchar(10)),4)
	set @V_DRIVER_PERIOD_MONTH = right(cast(@V_DRIVER_PERIOD as varchar(10)),2)
	set @V_DRIVER_PERIOD_FIRST_MONTH = cast(@V_DRIVER_PERIOD_YEAR+@V_DRIVER_PERIOD_MONTH+'01' as date);
--	select @V_DRIVER_PERIOD_YEAR, @V_DRIVER_PERIOD_MONTH,@V_DRIVER_PERIOD_FIRST_MONTH
	-------------------------|CREATE TEMP TABLE|---------------------------
	BEGIN TRY
	
	with total_policy_per_channel as (
		select 
			'join_key' join_key,
			a.channel,
			a.sharia_indicator,
			a.status,
			sum(total) as total
		from (
			--LIFE ASIA
			select 
				channel,
				sharia_indicator,
				'new_bussiness' status,
				count(1) total
				from FOND_ID.FOND_ETL5_tTEMP_LIFEASIA_DRIVER_DETAIL_NO_STAFF_PRODUCT
				where channel is not null and status='new_bussiness' and driver_period=@V_DRIVER_PERIOD
			group by 
				channel,
				sharia_indicator
				
			UNION ALL
			
			-- PRUAMAN
			select 
				'Bankassurance' channel,
				'Non Sharia' sharia_indicator,
				'new_bussiness' status,
				count(1) total
			from 
				--[NEWPRODUCTDB].[DBO].[MS_POLICY]
				STAG_ID.STAG_PRUAMAN_STAG_MS_POLICY 
			where 
				1= 1
				and YEAR(CREATED_DATE) = '2019' AND MONTH(CREATED_DATE) = @V_DRIVER_PERIOD_MONTH
				
			UNION ALL
			
			-- PRUAMAN SYARIAH
			select 
				'Bankassurance' channel,
				'Sharia' sharia_indicator,
				'new_bussiness' status,
				count(1) total
			from 
				--[NEWPRODUCTDB].[DBO].[MS_POLICY]
				STAG_ID.STAG_PRUSYARIAH_STAG_ETL4_POLICY
			where 
				1= 1
				and YEAR(DELIVERY_DATE) = '2019' AND MONTH(DELIVERY_DATE) = @V_DRIVER_PERIOD_MONTH
			
			UNION ALL
			
			-- Paylife
			select 
				'Bankassurance' channel,
				'Non Sharia' sharia_indicator,
				'new_bussiness' status,
				count(1) total
			from 
				STAG_ID.STAG_PAYLIFE_STAG_TBIILIFE_INSURED_FINAL
			where 
				1= 1
				and YEAR(UPLOAD_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(UPLOAD_DATE) = @V_DRIVER_PERIOD_MONTH
					
			UNION ALL
	
			-- Credit Shield		
			select 
				'Bankassurance' channel,
				'Non Sharia' sharia_indicator,
				'new_bussiness' status,
				count(1) total
			from 
				STAG_ID.STAG_CREDITSHIELD_STAG_PREMI 
			where 
				1= 1
				and YEAR(UPLOAD_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(UPLOAD_DATE) = @V_DRIVER_PERIOD_MONTH
				
			UNION ALL
	
			-- Pruemas		
			select 
				'Bankassurance' channel,
				'Non Sharia' sharia_indicator,
				'new_bussiness' status,
				count(1) total
			from 
				STAG_ID.STAG_PRUEMAS_STAG_ETL4_PREMIUM
			where 
				1= 1
				and YEAR(UPLOAD_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(UPLOAD_DATE) = @V_DRIVER_PERIOD_MONTH
					
			UNION ALL
			
			-- LIFE ASIA EXISTING BUSINESSS
			select 
				channel,
				sharia_indicator,
				'existing_bussiness' status,
				count(1) total
				from FOND_ID.FOND_ETL5_tTEMP_LIFEASIA_DRIVER_DETAIL_NO_STAFF_PRODUCT
				where channel is not null and status<>'new_bussiness' and driver_period=@V_DRIVER_PERIOD
			group by 
				channel,
				sharia_indicator
			
			UNION ALL
			
			-- PRUAMAN EXISTING BUSINESS
			select 
				'Bankassurance' channel,
				'Sharia' sharia_indicator,
				'existing_bussiness' status,
				count(1) total
			from 
				--[NEWPRODUCTDB].[DBO].[MS_POLICY]
				STAG_ID.STAG_PRUAMAN_STAG_MS_POLICY 
			where 
				1= 1
				and --YEAR(CREATED_DATE) < '2020' AND MONTH(CREATED_DATE) < @V_DRIVER_PERIOD_MONTH
--				 CREATED_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
				CREATED_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
			UNION ALL
			
			-- PRUAMAN SYARIAH EXISTING BUSINESS
			select 
				'Bankassurance' channel,
				'Non Sharia' sharia_indicator,
				'existing_bussiness' status,
				count(1) total
			from 
				STAG_ID.STAG_PRUSYARIAH_STAG_ETL4_POLICY
			where 
				1= 1
				and --YEAR(DELIVERY_DATE) < '2020' AND MONTH(DELIVERY_DATE) < @V_DRIVER_PERIOD_MONTH
--				DELIVERY_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
				DELIVERY_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
			UNION ALL 
			
			-- Paylife
			select 
				'Bankassurance' channel,
				'Non Sharia' sharia_indicator,
				'existing_bussiness' status,
				count(1) total
			from 
				STAG_ID.STAG_PAYLIFE_STAG_TBIILIFE_INSURED_FINAL
			where 
				1= 1
				and --YEAR(UPLOAD_DATE) < '2020' AND MONTH(UPLOAD_DATE) < @V_DRIVER_PERIOD_MONTH
--				UPLOAD_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
				UPLOAD_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
			UNION ALL
	
			-- Credit Shield		
			select 
				'Bankassurance' channel,
				'Non Sharia' sharia_indicator,
				'new_bussiness' status,
				count(1) total
			from 
				STAG_ID.STAG_CREDITSHIELD_STAG_PREMI 
			where 
				1= 1
				and 
--				UPLOAD_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
				UPLOAD_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
				--YEAR(UPLOAD_DATE)  < '2020'  AND MONTH(UPLOAD_DATE) < @V_DRIVER_PERIOD_MONTH	
				
			UNION ALL 
			-- Credit Shield		
			select 
				'Bankassurance' channel,
				'Non Sharia' sharia_indicator,
				'new_bussiness' status,
				count(1) total
			from 
				STAG_ID.STAG_PRUEMAS_STAG_ETL4_PREMIUM
			where 
				1= 1
				and 	
--				UPLOAD_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
				UPLOAD_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
				--YEAR(UPLOAD_DATE)  < '2020'  AND MONTH(UPLOAD_DATE) < @V_DRIVER_PERIOD_MONTH
			
			
		)a group by 
			a.channel,
			a.sharia_indicator,
			a.status
	)
	--select * from total_policy_per_channel
	select 
			a.*, 
			ISNULL(a.agency_non_sharia_new_bussiness/NULLIF(b.total,0),0) as cp_agency_non_sharia_new_bussiness ,
			ISNULL(a.agency_non_sharia_existing_bussiness/NULLIF(c.total,0),0) as cp_agency_non_sharia_existing_bussiness,
			ISNULL(a.bancassurance_non_sharia_new_bussiness/NULLIF(d.total,0),0) as cp_bancassurance_non_sharia_new_bussiness,
			ISNULL(a.bancassurance_non_sharia_existing_bussiness/NULLIF(e.total,0),0) as cp_bancassurance_non_sharia_existing_bussiness,
			ISNULL(a.dmtm_non_sharia_new_bussiness/NULLIF(f.total,0),0) as cp_dmtm_non_sharia_new_bussiness,
			ISNULL(a.dmtm_non_sharia_existing_bussiness/NULLIF(g.total,0),0) as cp_dmtm_non_sharia_existing_bussiness,
			ISNULL(a.agency_sharia_new_bussiness/NULLIF(h.total,0),0) as cp_agency_sharia_new_bussiness,
			ISNULL(a.agency_sharia_existing_bussiness/NULLIF(i.total,0),0) as cp_agency_sharia_existing_bussiness,
			ISNULL(a.bancassurance_sharia_new_bussiness/NULLIF(j.total,0),0) as cp_bancassurance_sharia_new_bussiness,
			ISNULL(a.bancassurance_sharia_existing_bussiness/NULLIF(k.total,0),0) as cp_bancassurance_sharia_existing_bussiness,
			ISNULL(a.dmtm_sharia_new_bussiness/NULLIF(l.total,0),0) as cp_dmtm_sharia_new_bussiness,
			ISNULL(a.dmtm_sharia_existing_bussiness/NULLIF(m.total,0),0)as cp_dmtm_sharia_existing_bussiness
		into 
			--[STAG_ID].STAG_ETL5_TEMP_TOTAL_POLICY_PER_CHANNEL
			#stag_etl5_temp_total_policy_per_channel
	from (select 'join_key' join_key,a.* from FOND_ID.FOND_ETL5_tTEMP_LIFEASIA_DRIVER_DETAIL_NO_STAFF_PRODUCT_DEPARTMENT a where accounting_period=@V_DRIVER_PERIOD )a 
		left join (select total,join_key from total_policy_per_channel where channel ='Agency' and sharia_indicator='Non Sharia' and status='new_bussiness' )b on a.join_key = b.join_key
		left join (select total,join_key from total_policy_per_channel where channel ='Agency' and sharia_indicator='Non Sharia' and status='existing_bussiness' )c on a.join_key = c.join_key
		left join (select total,join_key from total_policy_per_channel where channel ='Bankassurance' and sharia_indicator='Non Sharia' and status='new_bussiness' )d on a.join_key = d.join_key
		left join (select total,join_key from total_policy_per_channel where channel ='Bankassurance' and sharia_indicator='Non Sharia' and status='existing_bussiness' )e on a.join_key = e.join_key
		left join (select total,join_key from total_policy_per_channel where channel ='DMTM' and sharia_indicator='Non Sharia' and status='new_bussiness' )f on a.join_key = f.join_key
		left join (select total,join_key from total_policy_per_channel where channel ='DMTM' and sharia_indicator='Non Sharia' and status='existing_bussiness' )g on a.join_key = g.join_key
		left join (select total,join_key from total_policy_per_channel where channel ='Agency' and sharia_indicator='Sharia' and status='new_bussiness' )h on a.join_key = h.join_key
		left join (select total,join_key from total_policy_per_channel where channel ='Agency' and sharia_indicator='Sharia' and status='existing_bussiness' )i on a.join_key = i.join_key
		left join (select total,join_key from total_policy_per_channel where channel ='Bankassurance' and sharia_indicator='Sharia' and status='new_bussiness' )j on a.join_key = j.join_key
		left join (select total,join_key from total_policy_per_channel where channel ='Bankassurance' and sharia_indicator='Sharia' and status='existing_bussiness' )k on a.join_key = k.join_key
		left join (select total,join_key from total_policy_per_channel where channel ='DMTM' and sharia_indicator='Sharia' and status='new_bussiness' )l on a.join_key = l.join_key
		left join (select total,join_key from total_policy_per_channel where channel ='DMTM' and sharia_indicator='Sharia' and status='existing_bussiness' )m on a.join_key = m.join_key
	;
	print 'create stag_etl5_temp_total_policy_per_channel';
	--select * from #stag_etl5_temp_total_policy_per_channel
	
	--DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='2019011';
	--DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='2019';
	--DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='11';
	--DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='20191101';
	with tbl_a as (
		select
			'join_key' join_key,
			'LifeAsia' driver_source,
			product_desc,
			channel,
			sharia_indicator,
			count(1) total
		from
			--ifrs17.etl5_temp_no_staff_per_product a
			FOND_ID.FOND_ETL5_tTEMP_LIFEASIA_DRIVER_DETAIL_NO_STAFF_PRODUCT a
		where
			status = 'new_bussiness' 
			and channel is not null
			AND driver_period=@V_DRIVER_PERIOD
		group by
			product_desc,
			channel,
			sharia_indicator
	), policy_per_channel_new_bussiness as (
		select 
			a.*,
			b.goc,
			case 
				when channel='Agency' and sharia_indicator='Non Sharia' then b.cp_agency_non_sharia_new_bussiness
			end as cp_agency_non_sharia_new_bussiness,
			case 
				when channel='Agency' and sharia_indicator='Sharia' then  b.cp_agency_sharia_new_bussiness
			end as cp_agency_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Non Sharia' then b.cp_bancassurance_non_sharia_new_bussiness
			end as cp_bancassurance_non_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Sharia' then b.cp_bancassurance_sharia_new_bussiness
			end as cp_bancassurance_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Non Sharia' then b.cp_dmtm_non_sharia_new_bussiness
			end as cp_dmtm_non_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Sharia' then b.cp_dmtm_sharia_new_bussiness
			end as cp_dmtm_sharia_new_bussiness
		from 
		tbl_a a left join #stag_etl5_temp_total_policy_per_channel b
		--#etl5_temp_total_policy_per_channel b 
		on a.join_key = b.join_key
	)
	
	select 
			--cast(a.hoissdte as varchar(20)) as hoissdte,	
			--a.agent_cd,	
			policy_no,	
			a.product_cd,	
			a.benf_cd as benefit_cd,	
			a.status,	
			a.product_desc,	
			a.t0 as fund_cd,
			a.channel,
			a.sharia_indicator,
			b.driver_source,
			b.total as total_policy_per_product, 
			b.goc,
			cast(b.cp_agency_non_sharia_new_bussiness as numeric(18,3)) cp_agency_non_sharia_new_bussiness,
			cast(b.cp_agency_sharia_new_bussiness as numeric(18,3)) cp_agency_sharia_new_bussiness,
			cast(b.cp_bancassurance_non_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_non_sharia_new_bussiness,
			cast(b.cp_bancassurance_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_sharia_new_bussiness,
			cast(b.cp_dmtm_non_sharia_new_bussiness as numeric(18,3)) cp_dmtm_non_sharia_new_bussiness,
			cast(b.cp_dmtm_sharia_new_bussiness as numeric(18,3)) cp_dmtm_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_agency_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_agency_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_sharia_existing_bussiness
		into
			#etl5_temp_lifeasia_newbusiness
		from (
		select * from 
			--ifrs17.etl5_temp_no_staff_per_product 
			FOND_ID.FOND_ETL5_tTEMP_LIFEASIA_DRIVER_DETAIL_NO_STAFF_PRODUCT
		where status = 'new_bussiness'
		--and channel is not null
		AND driver_period=@V_DRIVER_PERIOD
	)a left join policy_per_channel_new_bussiness b on a.product_desc = b.product_desc 
		and a.channel = b.channel
		and a.sharia_indicator = b.sharia_indicator
	;
	
	
	--DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='2019011';
	--DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='2019';
	--DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='11';
	--DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='20191101';
	--Pruaman
	with tbl_pruaman as (
		select
			'join_key' join_key,
			'PruAman' driver_source,
			'PruAman' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			count(1) total
		from
			STAG_ID.STAG_PRUAMAN_STAG_MS_POLICY 
		where 
			1= 1
			and YEAR(CREATED_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(CREATED_DATE) = @V_DRIVER_PERIOD_MONTH
	), policy_per_channel_new_bussiness as (
		select 
			a.*,
			b.goc,
			case 
				when channel='Agency' and sharia_indicator='Non Sharia' then b.cp_agency_non_sharia_new_bussiness
			end as cp_agency_non_sharia_new_bussiness,
			case 
				when channel='Agency' and sharia_indicator='Sharia' then b.cp_agency_sharia_new_bussiness
			end as cp_agency_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Non Sharia' then b.cp_bancassurance_non_sharia_new_bussiness
			end as cp_bancassurance_non_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Sharia' then  b.cp_bancassurance_sharia_new_bussiness
			end as cp_bancassurance_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Non Sharia' then b.cp_dmtm_non_sharia_new_bussiness
			end as cp_dmtm_non_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Sharia' then b.cp_dmtm_sharia_new_bussiness
			end as cp_dmtm_sharia_new_bussiness
		from 
		tbl_pruaman a left join #stag_etl5_temp_total_policy_per_channel b on a.join_key = b.join_key
	)
	
	select 
			--'' hoissdte,	
			--'' agent_cd,	
			a.POLICY_ID as policy_no,	
			'PruAman' product_cd,	
			'PruAman' benefit_cd,	
			'new_bussiness' status,	
			'PruAman' product_desc,	
			'GTNN000' as fund_cd,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			b.driver_source,
			b.total as total_policy_per_product, 
			b.goc,
			cast(b.cp_agency_non_sharia_new_bussiness as numeric(18,3)) cp_agency_non_sharia_new_bussiness,
			cast(b.cp_agency_sharia_new_bussiness as numeric(18,3)) cp_agency_sharia_new_bussiness,
			cast(b.cp_bancassurance_non_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_non_sharia_new_bussiness,
			cast(b.cp_bancassurance_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_sharia_new_bussiness,
			cast(b.cp_dmtm_non_sharia_new_bussiness as numeric(18,3)) cp_dmtm_non_sharia_new_bussiness,
			cast(b.cp_dmtm_sharia_new_bussiness as numeric(18,3)) cp_dmtm_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_agency_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_agency_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_sharia_existing_bussiness
		into
			#etl5_temp_pruaman_newbusiness
		from (
		select 	
			'PruAman' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			a.* from 
			STAG_ID.STAG_PRUAMAN_STAG_MS_POLICY a
		where 
			1= 1
			and YEAR(CREATED_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(CREATED_DATE) = @V_DRIVER_PERIOD_MONTH
	)a left join policy_per_channel_new_bussiness b on a.product_desc = b.product_desc 
		and a.channel = b.channel
		and a.sharia_indicator = b.sharia_indicator
	;
	
	--DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='2019011';
	--DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='2019';
	--DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='11';
	--DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='20191101';
	--Pruaman S
	with tbl_pruamans as (
		select
			'join_key' join_key,
			'PruAmanS' driver_source,
			'PruAmanS' product_desc,
			'Bankassurance' channel,
			'Sharia' sharia_indicator,
			count(1) total
		from
			STAG_ID.STAG_PRUSYARIAH_STAG_ETL4_POLICY
		where 
			1= 1
			and YEAR(DELIVERY_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(DELIVERY_DATE) = @V_DRIVER_PERIOD_MONTH
	), policy_per_channel_new_bussiness as (
		select 
			a.*,
			b.goc,
			case 
				when channel='Agency' and sharia_indicator='Non Sharia' then  b.cp_agency_non_sharia_new_bussiness
			end as cp_agency_non_sharia_new_bussiness,
			case 
				when channel='Agency' and sharia_indicator='Sharia' then  b.cp_agency_sharia_new_bussiness
			end as cp_agency_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Non Sharia' then  b.cp_bancassurance_non_sharia_new_bussiness
			end as cp_bancassurance_non_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Sharia' then  b.cp_bancassurance_sharia_new_bussiness
			end as cp_bancassurance_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Non Sharia' then  b.cp_dmtm_non_sharia_new_bussiness
			end as cp_dmtm_non_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Sharia' then  b.cp_dmtm_sharia_new_bussiness
			end as cp_dmtm_sharia_new_bussiness
		from 
		tbl_pruamans a left join #stag_etl5_temp_total_policy_per_channel b on a.join_key = b.join_key
	)
	
	select 
			--'' hoissdte,	
			--'' agent_cd,	
			cast(POLICY_ID as varchar(36)) as policy_no,	
			'PruAmanS' product_cd,	
			'PruAmanS' benefit_cd,	
			'new_bussiness' status,	
			'PruAmanS' product_desc,	
			'GTNN000' as fund_cd,
			'Bankassurance' channel,
			'Sharia' sharia_indicator,
			b.driver_source,
			b.total as total_policy_per_product, 
			b.goc,
			cast(b.cp_agency_non_sharia_new_bussiness as numeric(18,3)) cp_agency_non_sharia_new_bussiness,
			cast(b.cp_agency_sharia_new_bussiness as numeric(18,3)) cp_agency_sharia_new_bussiness,
			cast(b.cp_bancassurance_non_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_non_sharia_new_bussiness,
			cast(b.cp_bancassurance_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_sharia_new_bussiness,
			cast(b.cp_dmtm_non_sharia_new_bussiness as numeric(18,3)) cp_dmtm_non_sharia_new_bussiness,
			cast(b.cp_dmtm_sharia_new_bussiness as numeric(18,3)) cp_dmtm_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_agency_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_agency_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_sharia_existing_bussiness
		into
			#etl5_temp_pruamans_newbusiness
		from (
		select 	
			'PruAmanS' product_desc,
			'Bankassurance' channel,
			'Sharia' sharia_indicator,
			a.* from 
			STAG_ID.STAG_PRUSYARIAH_STAG_ETL4_POLICY a
		where 
			1= 1
			and YEAR(DELIVERY_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(DELIVERY_DATE) = @V_DRIVER_PERIOD_MONTH
	)a left join policy_per_channel_new_bussiness b on a.product_desc = b.product_desc 
		and a.channel = b.channel
		and a.sharia_indicator = b.sharia_indicator
	;
	
	
	--- credit shield
	--DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='2019011';
	--DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='2019';
	--DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='11';
	--DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='20191101';
	with tbl_credit as (
		select
			'join_key' join_key,
			'CreditShield' driver_source,
			'CreditShield' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			count(1) total
		
		from 
			STAG_ID.STAG_CREDITSHIELD_STAG_PREMI
		where 
			1= 1
			and YEAR(UPLOAD_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(UPLOAD_DATE) = @V_DRIVER_PERIOD_MONTH
			
	), policy_per_channel_new_bussiness as (
		select 
			a.*,
			b.goc,
			case 
				when channel='Agency' and sharia_indicator='Non Sharia' then b.cp_agency_non_sharia_new_bussiness
			end as cp_agency_non_sharia_new_bussiness,
			case 
				when channel='Agency' and sharia_indicator='Sharia' then b.cp_agency_sharia_new_bussiness
			end as cp_agency_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Non Sharia' then b.cp_bancassurance_non_sharia_new_bussiness
			end as cp_bancassurance_non_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Sharia' then b.cp_bancassurance_sharia_new_bussiness
			end as cp_bancassurance_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Non Sharia' then b.cp_dmtm_non_sharia_new_bussiness
			end as cp_dmtm_non_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Sharia' then  b.cp_dmtm_sharia_new_bussiness
			end as cp_dmtm_sharia_new_bussiness
		from 
		tbl_credit a left join #stag_etl5_temp_total_policy_per_channel b on a.join_key = b.join_key
	)
	
	select 
			--'' hoissdte,	
			--'' agent_cd,	
			POLICY_NO,	
			'CreditShield' product_cd,	
			'CreditShield' benefit_cd,	
			'new_bussiness' status,	
			'CreditShield' product_desc,	
			'GTNN000' as fund_cd,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			b.driver_source,
			b.total as total_policy_per_product, 
			b.goc,
			cast(b.cp_agency_non_sharia_new_bussiness as numeric(18,3)) cp_agency_non_sharia_new_bussiness,
			cast(b.cp_agency_sharia_new_bussiness as numeric(18,3)) cp_agency_sharia_new_bussiness,
			cast(b.cp_bancassurance_non_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_non_sharia_new_bussiness,
			cast(b.cp_bancassurance_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_sharia_new_bussiness,
			cast(b.cp_dmtm_non_sharia_new_bussiness as numeric(18,3)) cp_dmtm_non_sharia_new_bussiness,
			cast(b.cp_dmtm_sharia_new_bussiness as numeric(18,3)) cp_dmtm_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_agency_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_agency_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_sharia_existing_bussiness
			into
			#etl5_temp_creditshield_newbusiness
		from (
		select 	
			'CreditShield' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			a.* 	
		from 
			STAG_ID.STAG_CREDITSHIELD_STAG_PREMI a
		where 
			1= 1
			and YEAR(UPLOAD_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(UPLOAD_DATE) = @V_DRIVER_PERIOD_MONTH
	)a left join policy_per_channel_new_bussiness b on a.product_desc = b.product_desc 
		and a.channel = b.channel
		and a.sharia_indicator = b.sharia_indicator
	;
	
	
	
	--- paylife
	--DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='2019011';
	--DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='2019';
	--DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='11';
	--DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='20191101';
	with tbl_paylife as (
		select
			'join_key' join_key,
			'PayLife' driver_source,
			'PayLife' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			count(1) total
		from 
			STAG_ID.STAG_PAYLIFE_STAG_TBIILIFE_INSURED_FINAL
		where 
			1= 1
			and YEAR(UPLOAD_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(UPLOAD_DATE) = @V_DRIVER_PERIOD_MONTH
			
	), policy_per_channel_new_bussiness as (
		select 
			a.*,
			b.goc,
			case 
				when channel='Agency' and sharia_indicator='Non Sharia' then  b.cp_agency_non_sharia_new_bussiness
			end as cp_agency_non_sharia_new_bussiness,
			case 
				when channel='Agency' and sharia_indicator='Sharia' then  b.cp_agency_sharia_new_bussiness
			end as cp_agency_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Non Sharia' then  b.cp_bancassurance_non_sharia_new_bussiness
			end as cp_bancassurance_non_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Sharia' then  b.cp_bancassurance_sharia_new_bussiness
			end as cp_bancassurance_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Non Sharia' then  b.cp_dmtm_non_sharia_new_bussiness
			end as cp_dmtm_non_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Sharia' then  b.cp_dmtm_sharia_new_bussiness
			end as cp_dmtm_sharia_new_bussiness
		from 
		tbl_paylife a left join #stag_etl5_temp_total_policy_per_channel b on a.join_key = b.join_key
	)
	
	select 
			--'' hoissdte,	
			--'' agent_cd,	
			Cast(INSURED_ID AS VARCHAR(8)) as policy_no,	
			'PayLife' product_cd,	
			'PayLife' benefit_cd,	
			'new_bussiness' status,	
			'PayLife' product_desc,	
			'GTNN000' as fund_cd,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			b.driver_source,
			b.total as total_policy_per_product, 
			b.goc,
			cast(b.cp_agency_non_sharia_new_bussiness as numeric(18,3)) cp_agency_non_sharia_new_bussiness,
			cast(b.cp_agency_sharia_new_bussiness as numeric(18,3)) cp_agency_sharia_new_bussiness,
			cast(b.cp_bancassurance_non_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_non_sharia_new_bussiness,
			cast(b.cp_bancassurance_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_sharia_new_bussiness,
			cast(b.cp_dmtm_non_sharia_new_bussiness as numeric(18,3)) cp_dmtm_non_sharia_new_bussiness,
			cast(b.cp_dmtm_sharia_new_bussiness as numeric(18,3)) cp_dmtm_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_agency_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_agency_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_sharia_existing_bussiness
		into
			#etl5_temp_paylife_newbusiness
		from (
		select 	
			'PayLife' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			a.* 	
		from 
			STAG_ID.STAG_PAYLIFE_STAG_TBIILIFE_INSURED_FINAL a
		where 
			1= 1
			and YEAR(UPLOAD_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(UPLOAD_DATE) = @V_DRIVER_PERIOD_MONTH
	)a left join policy_per_channel_new_bussiness b on a.product_desc = b.product_desc 
		and a.channel = b.channel
		and a.sharia_indicator = b.sharia_indicator
		
	;
	
	
	--- PruEmas
	--DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='2019011';
	--DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='2019';
	--DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='11';
	--DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='20191101';
	with tbl_pruemas as (
		select
			'join_key' join_key,
			'PruEmas' driver_source,
			'PruEmas' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			count(1) total
		from 
				STAG_ID.STAG_PRUEMAS_STAG_ETL4_PREMIUM
			where 
				1= 1
				and YEAR(UPLOAD_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(UPLOAD_DATE) = @V_DRIVER_PERIOD_MONTH
			
	), policy_per_channel_new_bussiness as (
		select 
			a.*,
			b.goc,
			case 
				when channel='Agency' and sharia_indicator='Non Sharia' then  b.cp_agency_non_sharia_new_bussiness
			end as cp_agency_non_sharia_new_bussiness,
			case 
				when channel='Agency' and sharia_indicator='Sharia' then  b.cp_agency_sharia_new_bussiness
			end as cp_agency_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Non Sharia' then  b.cp_bancassurance_non_sharia_new_bussiness
			end as cp_bancassurance_non_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Sharia' then  b.cp_bancassurance_sharia_new_bussiness
			end as cp_bancassurance_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Non Sharia' then  b.cp_dmtm_non_sharia_new_bussiness
			end as cp_dmtm_non_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Sharia' then  b.cp_dmtm_sharia_new_bussiness
			end as cp_dmtm_sharia_new_bussiness
		from 
		tbl_pruemas a left join #stag_etl5_temp_total_policy_per_channel b on a.join_key = b.join_key
	)
	
	select 
			--'' hoissdte,	
			--' agent_cd,	
			Cast(ID AS VARCHAR(8)) as policy_no,	
			'PruEmas' product_cd,	
			'PruEmas' benefit_cd,	
			'new_bussiness' status,	
			'PruEmas' product_desc,	
			'GTNN000' as fund_cd,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			b.driver_source,
			b.total as total_policy_per_product, 
			b.goc,
			cast(b.cp_agency_non_sharia_new_bussiness as numeric(18,3)) cp_agency_non_sharia_new_bussiness,
			cast(b.cp_agency_sharia_new_bussiness as numeric(18,3)) cp_agency_sharia_new_bussiness,
			cast(b.cp_bancassurance_non_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_non_sharia_new_bussiness,
			cast(b.cp_bancassurance_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_sharia_new_bussiness,
			cast(b.cp_dmtm_non_sharia_new_bussiness as numeric(18,3)) cp_dmtm_non_sharia_new_bussiness,
			cast(b.cp_dmtm_sharia_new_bussiness as numeric(18,3)) cp_dmtm_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_agency_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_agency_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_sharia_existing_bussiness
		into
			#etl5_temp_pruemas_newbusiness
		from (
		select 	
			'PruEmas' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			a.* 	
		from 
			STAG_ID.STAG_PRUEMAS_STAG_ETL4_PREMIUM a
			where 
			1= 1
			and YEAR(UPLOAD_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(UPLOAD_DATE) = @V_DRIVER_PERIOD_MONTH
	)a left join policy_per_channel_new_bussiness b on a.product_desc = b.product_desc 
		and a.channel = b.channel
		and a.sharia_indicator = b.sharia_indicator
	;
	
	
	
	--DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='2019011';
	--DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='2019';
	--DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='11';
	--DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='20191101';
	--DECLARE  @V_id varchar(10)= cast(FORMAT(GETDATE(),'mm') as varchar)
	DECLARE  @V_TNAME_NB VARCHAR(100) = 'FOND_ID.FOND_ETL5_tTEMP_NO_STAFF_PRODUCT_NB_'+@V_DRIVER_PERIOD
	set @NB_OUT=@V_TNAME_NB
	DECLARE @sql_dropTNB VARCHAR(8000)='if object_id ('+''''+@V_TNAME_NB+''''+','+'''U'''+') is not null DROP TABLE '+@V_TNAME_NB+';'
	,       @sql_createTNB VARCHAR(8000)='CREATE TABLE '+@V_TNAME_NB+' WITH( DISTRIBUTION = ROUND_ROBIN ,CLUSTERED COLUMNSTORE INDEX) AS '
	,       @sql_selectTNB VARCHAR(8000)=
	'
		select  cast(policy_no as varchar(36)) COLLATE DATABASE_DEFAULT as policy_no, product_cd COLLATE DATABASE_DEFAULT product_cd, benefit_cd COLLATE DATABASE_DEFAULT benefit_cd, status COLLATE DATABASE_DEFAULT status, product_desc COLLATE DATABASE_DEFAULT product_desc, fund_cd COLLATE DATABASE_DEFAULT fund_cd, channel COLLATE DATABASE_DEFAULT channel, sharia_indicator COLLATE DATABASE_DEFAULT sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness
		from #etl5_temp_lifeasia_newbusiness
		union all 
		select cast(policy_no as varchar(36)) COLLATE DATABASE_DEFAULT as policy_no, product_cd COLLATE DATABASE_DEFAULT product_cd, benefit_cd COLLATE DATABASE_DEFAULT benefit_cd, status COLLATE DATABASE_DEFAULT status, product_desc COLLATE DATABASE_DEFAULT product_desc, fund_cd COLLATE DATABASE_DEFAULT fund_cd, channel COLLATE DATABASE_DEFAULT channel, sharia_indicator COLLATE DATABASE_DEFAULT sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness 
		from #etl5_temp_pruaman_newbusiness 
		union all 
		select cast(policy_no as varchar(36)) COLLATE DATABASE_DEFAULT as policy_no, product_cd, benefit_cd, status, product_desc, fund_cd, channel, sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness 
		from #etl5_temp_pruamans_newbusiness 
		union all 
		select policy_no, product_cd, benefit_cd, status, product_desc, fund_cd, channel, sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness 
		from #etl5_temp_paylife_newbusiness 
		union all 
		select POLICY_NO , product_cd, benefit_cd, status, product_desc, fund_cd, channel, sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness 
		from #etl5_temp_creditshield_newbusiness
		union all 
		select policy_no, product_cd, benefit_cd, status, product_desc, fund_cd, channel, sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness 
		from #etl5_temp_pruemas_newbusiness
		
	';
	--Drop Table
	EXEC (@sql_dropTNB);
	--Create Dynamic table for 
	EXEC( @sql_createTNB+@sql_selectTNB);
	
	print 'create '+@V_TNAME_NB
	
	-------------------------|DROP TEMP TABLE EXISTING BUSINESS|---------------------------
		 IF OBJECT_ID('tempdb..#etl5_temp_lifeasia_existingbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_lifeasia_existingbusiness
		END;
	
		 IF OBJECT_ID('tempdb..#etl5_temp_pruaman_existingbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_pruaman_existingbusiness
		END;
	
		IF OBJECT_ID('tempdb..#etl5_temp_pruamans_existingbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_pruamans_existingbusiness
		END;
	
		IF OBJECT_ID('tempdb..#etl5_temp_paylife_existingbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_paylife_existingbusiness
		END;
	
		IF OBJECT_ID('tempdb..#etl5_temp_creditshield_existingbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_creditshield_existingbusiness
		END;
	
		IF OBJECT_ID('tempdb..#etl5_temp_pruemas_existingbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_pruemas_existingbusiness
		END;
		
	
	------------continue sql server(existing bussiness)-----
	--DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='2019011';
	--DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='2019';
	--DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='11';
	--DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='20191101';
	
	--drop table ifrs17.etl5_temp_existing_bussiness ;
	--create table ifrs17.etl5_temp_existing_bussiness as
	with tbl_a as (
		select
			'join_key' join_key,
			product_desc,
			channel,
			sharia_indicator,
			count(1) total
		from
			--ifrs17.etl5_temp_no_staff_per_product a
			FOND_ID.FOND_ETL5_tTEMP_LIFEASIA_DRIVER_DETAIL_NO_STAFF_PRODUCT a
		where
			status <> 'new_bussiness' 
			and channel is not null
			AND driver_period=@V_DRIVER_PERIOD
		group by
			product_desc,
			channel,
			sharia_indicator
	), policy_per_channel_existing_bussiness as (
		select 
			a.*,
			b.goc,
			case 
				when channel='Agency' and sharia_indicator='Non Sharia' then  b.cp_agency_non_sharia_existing_bussiness
			end as cp_agency_non_sharia_existing_bussiness,
			case 
				when channel='Agency' and sharia_indicator='Sharia' then  b.cp_agency_sharia_existing_bussiness
			end as cp_agency_sharia_existing_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Non Sharia' then  b.cp_bancassurance_non_sharia_existing_bussiness
			end as cp_bancassurance_non_sharia_existing_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Sharia' then  b.cp_bancassurance_sharia_existing_bussiness
			end as cp_bancassurance_sharia_existing_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Non Sharia' then  b.cp_dmtm_non_sharia_existing_bussiness
			end as cp_dmtm_non_sharia_existing_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Sharia' then  b.cp_dmtm_sharia_existing_bussiness
			end as cp_dmtm_sharia_existing_bussiness
		from 
		tbl_a a left join #stag_etl5_temp_total_policy_per_channel b on a.join_key = b.join_key
	)
	
	
	select 
			--cast(a.hoissdte as varchar(20)) as hoissdte,	
			--a.agent_cd,	
			policy_no,	
			a.product_cd,	
			a.benf_cd as benefit_cd,	
			a.status,	
			a.product_desc,	
			a.t0 as fund_cd,
			a.channel,
			a.sharia_indicator,
			'LifeAsia' driver_source,
			b.total as total_policy_per_product, 
			b.goc,
			cast(null as numeric(18,3)) cp_agency_non_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_agency_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_non_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_non_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_sharia_new_bussiness,
			cast(b.cp_agency_non_sharia_existing_bussiness as numeric(18,3)) cp_agency_non_sharia_existing_bussiness,
			cast(b.cp_agency_sharia_existing_bussiness as numeric(18,3)) cp_agency_sharia_existing_bussiness,
			cast(b.cp_bancassurance_non_sharia_existing_bussiness as numeric(18,3)) cp_bancassurance_non_sharia_existing_bussiness,
			cast(b.cp_bancassurance_sharia_existing_bussiness as numeric(18,3)) cp_bancassurance_sharia_existing_bussiness,
			cast(b.cp_dmtm_non_sharia_existing_bussiness as numeric(18,3)) cp_dmtm_non_sharia_existing_bussiness,
			cast(b.cp_dmtm_sharia_existing_bussiness as numeric(18,3)) cp_dmtm_sharia_existing_bussiness
		into
			#etl5_temp_lifeasia_existingbusiness
		from (
		select * from 
			--ifrs17.etl5_temp_no_staff_per_product 
			FOND_ID.FOND_ETL5_tTEMP_LIFEASIA_DRIVER_DETAIL_NO_STAFF_PRODUCT
		where status <> 'new_bussiness'
		--and channel is not null
		AND driver_period=@V_DRIVER_PERIOD
	)a left join policy_per_channel_existing_bussiness b on a.product_desc = b.product_desc 
		and a.channel = b.channel
		and a.sharia_indicator = b.sharia_indicator
	;
	
	--Pruaman
	--DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='2019011';
	--DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='2019';
	--DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='11';
	--DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='20191101';
	with tbl_pruaman as (
		select
			'join_key' join_key,
			'PruAman' driver_source,
			'PruAman' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			count(1) total
		from
			STAG_ID.STAG_PRUAMAN_STAG_MS_POLICY 
		where 
			1= 1
			--and YEAR(CREATED_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(CREATED_DATE) = @V_DRIVER_PERIOD_MONTH
--			AND CREATED_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
			AND CREATED_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
	), policy_per_channel_new_bussiness as (
		select 
			a.*,
			b.goc,
			case 
				when channel='Agency' and sharia_indicator='Non Sharia' then  b.cp_agency_non_sharia_new_bussiness
			end as cp_agency_non_sharia_new_bussiness,
			case 
				when channel='Agency' and sharia_indicator='Sharia' then  b.cp_agency_sharia_new_bussiness
			end as cp_agency_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Non Sharia' then  b.cp_bancassurance_non_sharia_new_bussiness
			end as cp_bancassurance_non_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Sharia' then  b.cp_bancassurance_sharia_new_bussiness
			end as cp_bancassurance_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Non Sharia' then  b.cp_dmtm_non_sharia_new_bussiness
			end as cp_dmtm_non_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Sharia' then  b.cp_dmtm_sharia_new_bussiness
			end as cp_dmtm_sharia_new_bussiness
		from 
		tbl_pruaman a left join #stag_etl5_temp_total_policy_per_channel b on a.join_key = b.join_key
	)
	
	select 
			--'' hoissdte,	
			--'' agent_cd,	
			POLICY_ID as policy_no,	
			'PruAman' product_cd,	
			'PruAman' benefit_cd,	
			'new_bussiness' status,	
			'PruAman' product_desc,	
			'GTNN000' as fund_cd,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			b.driver_source,
			b.total as total_policy_per_product, 
			b.goc,
			cast(b.cp_agency_non_sharia_new_bussiness as numeric(18,3)) cp_agency_non_sharia_new_bussiness,
			cast(b.cp_agency_sharia_new_bussiness as numeric(18,3)) cp_agency_sharia_new_bussiness,
			cast(b.cp_bancassurance_non_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_non_sharia_new_bussiness,
			cast(b.cp_bancassurance_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_sharia_new_bussiness,
			cast(b.cp_dmtm_non_sharia_new_bussiness as numeric(18,3)) cp_dmtm_non_sharia_new_bussiness,
			cast(b.cp_dmtm_sharia_new_bussiness as numeric(18,3)) cp_dmtm_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_agency_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_agency_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_sharia_existing_bussiness
		into
			#etl5_temp_pruaman_existingbusiness
		from (
		select 	
			'PruAman' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			a.* from 
			STAG_ID.STAG_PRUAMAN_STAG_MS_POLICY a
		where 
			1= 1
			--and YEAR(CREATED_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(CREATED_DATE) = @V_DRIVER_PERIOD_MONTH
			AND 
--			CREATED_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
			CREATED_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
	)a left join policy_per_channel_new_bussiness b on a.product_desc = b.product_desc 
		and a.channel = b.channel
		and a.sharia_indicator = b.sharia_indicator
	;
	
	--Pruaman S
	with tbl_pruamans as (
		select
			'join_key' join_key,
			'PruAmanS' driver_source,
			'PruAmanS' product_desc,
			'Bankassurance' channel,
			'Sharia' sharia_indicator,
			count(1) total
		from
			STAG_ID.STAG_PRUSYARIAH_STAG_ETL4_POLICY
		where 
			1= 1
			--and YEAR(DELIVERY_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(DELIVERY_DATE) = @V_DRIVER_PERIOD_MONTH
			AND 
--			DELIVERY_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
			DELIVERY_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
	), policy_per_channel_new_bussiness as (
		select 
			a.*,
			b.goc,
			case 
				when channel='Agency' and sharia_indicator='Non Sharia' then  b.cp_agency_non_sharia_new_bussiness
			end as cp_agency_non_sharia_new_bussiness,
			case 
				when channel='Agency' and sharia_indicator='Sharia' then  b.cp_agency_sharia_new_bussiness
			end as cp_agency_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Non Sharia' then  b.cp_bancassurance_non_sharia_new_bussiness
			end as cp_bancassurance_non_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Sharia' then  b.cp_bancassurance_sharia_new_bussiness
			end as cp_bancassurance_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Non Sharia' then  b.cp_dmtm_non_sharia_new_bussiness
			end as cp_dmtm_non_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Sharia' then  b.cp_dmtm_sharia_new_bussiness
			end as cp_dmtm_sharia_new_bussiness
		from 
		tbl_pruamans a left join #stag_etl5_temp_total_policy_per_channel b on a.join_key = b.join_key
	)
	
	select 
			--'' hoissdte,	
			--'' agent_cd,	
			cast(POLICY_ID  as varchar(36)) as policy_no,	
			'PruAmanS' product_cd,	
			'PruAmanS' benefit_cd,	
			'new_bussiness' status,	
			'PruAmanS' product_desc,	
			'GTNN000' as fund_cd,
			'Bankassurance' channel,
			'Sharia' sharia_indicator,
			b.driver_source,
			b.total as total_policy_per_product, 
			b.goc,
			cast(b.cp_agency_non_sharia_new_bussiness as numeric(18,3)) cp_agency_non_sharia_new_bussiness,
			cast(b.cp_agency_sharia_new_bussiness as numeric(18,3)) cp_agency_sharia_new_bussiness,
			cast(b.cp_bancassurance_non_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_non_sharia_new_bussiness,
			cast(b.cp_bancassurance_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_sharia_new_bussiness,
			cast(b.cp_dmtm_non_sharia_new_bussiness as numeric(18,3)) cp_dmtm_non_sharia_new_bussiness,
			cast(b.cp_dmtm_sharia_new_bussiness as numeric(18,3)) cp_dmtm_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_agency_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_agency_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_sharia_existing_bussiness
		into
			#etl5_temp_pruamans_existingbusiness
		from (
		select 	
			'PruAmanS' product_desc,
			'Bankassurance' channel,
			'Sharia' sharia_indicator,
			a.* from 
			STAG_ID.STAG_PRUSYARIAH_STAG_ETL4_POLICY a
		where 
			1= 1
			--and YEAR(DELIVERY_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(DELIVERY_DATE) = @V_DRIVER_PERIOD_MONTH
			AND 
--			DELIVERY_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
			DELIVERY_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
	)a left join policy_per_channel_new_bussiness b on a.product_desc = b.product_desc 
		and a.channel = b.channel
		and a.sharia_indicator = b.sharia_indicator
	;
	
	
	--- credit shield
	--DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='2019011';
	--DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='2019';
	--DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='11';
	--DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='20191101';
	with tbl_credit as (
		select
			'join_key' join_key,
			'CreditShield' driver_source,
			'CreditShield' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			count(1) total
		
		from 
			STAG_ID.STAG_CREDITSHIELD_STAG_PREMI
		where 
			1= 1
			--and YEAR(UPLOAD_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(UPLOAD_DATE) = @V_DRIVER_PERIOD_MONTH
			AND 
--			UPLOAD_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
			UPLOAD_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
	), policy_per_channel_new_bussiness as (
		select 
			a.*,
			b.goc,
			case 
				when channel='Agency' and sharia_indicator='Non Sharia' then  b.cp_agency_non_sharia_new_bussiness
			end as cp_agency_non_sharia_new_bussiness,
			case 
				when channel='Agency' and sharia_indicator='Sharia' then  b.cp_agency_sharia_new_bussiness
			end as cp_agency_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Non Sharia' then  b.cp_bancassurance_non_sharia_new_bussiness
			end as cp_bancassurance_non_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Sharia' then  b.cp_bancassurance_sharia_new_bussiness
			end as cp_bancassurance_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Non Sharia' then  b.cp_dmtm_non_sharia_new_bussiness
			end as cp_dmtm_non_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Sharia' then  b.cp_dmtm_sharia_new_bussiness
			end as cp_dmtm_sharia_new_bussiness
		from 
		tbl_credit a left join #stag_etl5_temp_total_policy_per_channel b on a.join_key = b.join_key
	)
	
	select 
			--'' hoissdte,	
			--'' agent_cd,	
			POLICY_NO,	
			'CreditShield' product_cd,	
			'CreditShield' benefit_cd,	
			'new_bussiness' status,	
			'CreditShield' product_desc,
			'GTNN000' as fund_cd,		
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			b.driver_source,
			b.total as total_policy_per_product, 
			b.goc,
			cast(b.cp_agency_non_sharia_new_bussiness as numeric(18,3)) cp_agency_non_sharia_new_bussiness,
			cast(b.cp_agency_sharia_new_bussiness as numeric(18,3)) cp_agency_sharia_new_bussiness,
			cast(b.cp_bancassurance_non_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_non_sharia_new_bussiness,
			cast(b.cp_bancassurance_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_sharia_new_bussiness,
			cast(b.cp_dmtm_non_sharia_new_bussiness as numeric(18,3)) cp_dmtm_non_sharia_new_bussiness,
			cast(b.cp_dmtm_sharia_new_bussiness as numeric(18,3)) cp_dmtm_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_agency_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_agency_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_sharia_existing_bussiness
		into
			#etl5_temp_creditshield_existingbusiness
		from (
		select 	
			'CreditShield' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			a.* 	
		from 
			STAG_ID.STAG_CREDITSHIELD_STAG_PREMI a
		where 
			1= 1
			--and YEAR(UPLOAD_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(UPLOAD_DATE) = @V_DRIVER_PERIOD_MONTH
			AND 
--			UPLOAD_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
			UPLOAD_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
	)a left join policy_per_channel_new_bussiness b on a.product_desc = b.product_desc 
		and a.channel = b.channel
		and a.sharia_indicator = b.sharia_indicator
	;
	
	
	
	--- paylife
	--DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='2019011';
	--DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='2019';
	--DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='11';
	--DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='20191101';
	with tbl_paylife as (
		select
			'join_key' join_key,
			'PayLife' driver_source,
			'PayLife' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			count(1) total
		 from 
			STAG_ID.STAG_PAYLIFE_STAG_TBIILIFE_INSURED_FINAL
			--AZUREDWDEV.FOND_ID.FOND_PAYLIFE_DRIVER_DETAIL
		where 
			1= 1
			--and YEAR(UPLOAD_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(UPLOAD_DATE) = @V_DRIVER_PERIOD_MONTH
			--AND UPLOAD_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
			and cast(year(cast(UPLOAD_DATE as date)) as varchar(10))=@V_DRIVER_PERIOD_YEAR and cast(month(cast(UPLOAD_DATE as date)) as varchar(10))=@V_DRIVER_PERIOD_MONTH
			
	), policy_per_channel_new_bussiness as (
		select 
			a.*,
			b.goc,
			case 
				when channel='Agency' and sharia_indicator='Non Sharia' then  b.cp_agency_non_sharia_new_bussiness
			end as cp_agency_non_sharia_new_bussiness,
			case 
				when channel='Agency' and sharia_indicator='Sharia' then  b.cp_agency_sharia_new_bussiness
			end as cp_agency_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Non Sharia' then  b.cp_bancassurance_non_sharia_new_bussiness
			end as cp_bancassurance_non_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Sharia' then  b.cp_bancassurance_sharia_new_bussiness
			end as cp_bancassurance_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Non Sharia' then  b.cp_dmtm_non_sharia_new_bussiness
			end as cp_dmtm_non_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Sharia' then  b.cp_dmtm_sharia_new_bussiness
			end as cp_dmtm_sharia_new_bussiness
		from 
		tbl_paylife a left join #stag_etl5_temp_total_policy_per_channel b on a.join_key = b.join_key
	)
	
	select 
			--'' hoissdte,	
			--'' agent_cd,	
			Cast(INSURED_ID AS VARCHAR(8)) as policy_no,	
			'PayLife' product_cd,	
			'PayLife' benefit_cd,	
			'new_bussiness' status,	
			'PayLife' product_desc,
			'GTNN000' as fund_cd,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			b.driver_source,
			b.total as total_policy_per_product, 
			b.goc,
			cast(b.cp_agency_non_sharia_new_bussiness as numeric(18,3)) cp_agency_non_sharia_new_bussiness,
			cast(b.cp_agency_sharia_new_bussiness as numeric(18,3)) cp_agency_sharia_new_bussiness,
			cast(b.cp_bancassurance_non_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_non_sharia_new_bussiness,
			cast(b.cp_bancassurance_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_sharia_new_bussiness,
			cast(b.cp_dmtm_non_sharia_new_bussiness as numeric(18,3)) cp_dmtm_non_sharia_new_bussiness,
			cast(b.cp_dmtm_sharia_new_bussiness as numeric(18,3)) cp_dmtm_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_agency_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_agency_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_sharia_existing_bussiness
		into
			#etl5_temp_paylife_existingbusiness
		from (
		select 	
			'PayLife' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			a.* 	
		from 
			STAG_ID.STAG_PAYLIFE_STAG_TBIILIFE_INSURED_FINAL a
		where 
			1= 1
			--and YEAR(UPLOAD_DATE) = @V_DRIVER_PERIOD_YEAR AND MONTH(UPLOAD_DATE) = @V_DRIVER_PERIOD_MONTH
			AND 
--			UPLOAD_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
			UPLOAD_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
	)a left join policy_per_channel_new_bussiness b on a.product_desc = b.product_desc 
		and a.channel = b.channel
		and a.sharia_indicator = b.sharia_indicator
		
	;
	
	
	--- PruEmas
	
	with tbl_pruemas as (
		select
			'join_key' join_key,
			'PruEmas' driver_source,
			'PruEmas' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			count(1) total
		from 
				STAG_ID.STAG_PRUEMAS_STAG_ETL4_PREMIUM
			where 
				1= 1
				AND 
--				UPLOAD_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
				UPLOAD_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
	), policy_per_channel_new_bussiness as (
		select 
			a.*,
			b.goc,
			case 
				when channel='Agency' and sharia_indicator='Non Sharia' then  b.cp_agency_non_sharia_new_bussiness
			end as cp_agency_non_sharia_new_bussiness,
			case 
				when channel='Agency' and sharia_indicator='Sharia' then  b.cp_agency_sharia_new_bussiness
			end as cp_agency_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Non Sharia' then  b.cp_bancassurance_non_sharia_new_bussiness
			end as cp_bancassurance_non_sharia_new_bussiness,
			case 
				when channel='Bankassurance' and sharia_indicator='Sharia' then  b.cp_bancassurance_sharia_new_bussiness
			end as cp_bancassurance_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Non Sharia' then  b.cp_dmtm_non_sharia_new_bussiness
			end as cp_dmtm_non_sharia_new_bussiness,
			case 
				when channel='DMTM' and sharia_indicator='Sharia' then  b.cp_dmtm_sharia_new_bussiness
			end as cp_dmtm_sharia_new_bussiness
		from 
		tbl_pruemas a left join #stag_etl5_temp_total_policy_per_channel b on a.join_key = b.join_key
	)
	
	select 
			--'' hoissdte,	
			--'' agent_cd,	
			Cast(ID AS VARCHAR(8)) as policy_no,	
			'PruEmas' product_cd,	
			'PruEmas' benefit_cd,	
			'new_bussiness' status,	
			'PruEmas' product_desc,	
			'GTNN000' as fund_cd,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			b.driver_source,
			b.total as total_policy_per_product, 
			b.goc,
			cast(b.cp_agency_non_sharia_new_bussiness as numeric(18,3)) cp_agency_non_sharia_new_bussiness,
			cast(b.cp_agency_sharia_new_bussiness as numeric(18,3)) cp_agency_sharia_new_bussiness,
			cast(b.cp_bancassurance_non_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_non_sharia_new_bussiness,
			cast(b.cp_bancassurance_sharia_new_bussiness as numeric(18,3)) cp_bancassurance_sharia_new_bussiness,
			cast(b.cp_dmtm_non_sharia_new_bussiness as numeric(18,3)) cp_dmtm_non_sharia_new_bussiness,
			cast(b.cp_dmtm_sharia_new_bussiness as numeric(18,3)) cp_dmtm_sharia_new_bussiness,
			cast(null as numeric(18,3)) cp_agency_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_agency_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_bancassurance_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_non_sharia_existing_bussiness,
			cast(null as numeric(18,3)) cp_dmtm_sharia_existing_bussiness
		into
			#etl5_temp_pruemas_existingbusiness
		from (
		select 	
			'PruEmas' product_desc,
			'Bankassurance' channel,
			'Non Sharia' sharia_indicator,
			a.* 	
		from 
			STAG_ID.STAG_PRUEMAS_STAG_ETL4_PREMIUM a
			where 1= 1
--			AND UPLOAD_DATE < CAST(@V_DRIVER_PERIOD_FIRST_MONTH AS DATE)
			and UPLOAD_DATE < @V_DRIVER_PERIOD_FIRST_MONTH
	)a left join policy_per_channel_new_bussiness b on a.product_desc = b.product_desc 
		and a.channel = b.channel
		and a.sharia_indicator = b.sharia_indicator
	;
	
	
--	DECLARE  @V_DRIVER_PERIOD VARCHAR(10) ='2019011';
--	DECLARE  @V_DRIVER_PERIOD_YEAR VARCHAR(10) ='2019';
--	DECLARE  @V_DRIVER_PERIOD_MONTH VARCHAR(10) ='11';
--	DECLARE  @V_DRIVER_PERIOD_FIRST_MONTH VARCHAR(10) ='20191101';
	--DECLARE  @V_id varchar(10)= cast(FORMAT(GETDATE(),'mm') as varchar)
	DECLARE  @V_TNAME_EB VARCHAR(100) = 'FOND_ID.FOND_ETL5_tTEMP_NO_STAFF_PRODUCT_EB_'+@V_DRIVER_PERIOD
	set @EB_OUT=@V_TNAME_EB
	DECLARE @sql_dropTEB VARCHAR(8000)='if object_id ('+''''+@V_TNAME_EB+''''+','+'''U'''+') is not null DROP TABLE '+@V_TNAME_EB+';'
	,       @sql_createTEB VARCHAR(8000)='CREATE TABLE '+@V_TNAME_EB+' WITH( DISTRIBUTION = ROUND_ROBIN ,CLUSTERED COLUMNSTORE INDEX) AS '
	,       @sql_selectTEB VARCHAR(8000)=
	'
		select  cast(policy_no as varchar(36)) COLLATE DATABASE_DEFAULT as policy_no, product_cd COLLATE DATABASE_DEFAULT product_cd, benefit_cd COLLATE DATABASE_DEFAULT benefit_cd, status COLLATE DATABASE_DEFAULT status, product_desc COLLATE DATABASE_DEFAULT product_desc, fund_cd COLLATE DATABASE_DEFAULT fund_cd, channel COLLATE DATABASE_DEFAULT channel, sharia_indicator COLLATE DATABASE_DEFAULT sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness
		from #etl5_temp_lifeasia_existingbusiness
		union all 
		select cast(policy_no as varchar(36)) COLLATE DATABASE_DEFAULT as policy_no, product_cd COLLATE DATABASE_DEFAULT product_cd, benefit_cd COLLATE DATABASE_DEFAULT benefit_cd, status COLLATE DATABASE_DEFAULT status, product_desc COLLATE DATABASE_DEFAULT product_desc, fund_cd COLLATE DATABASE_DEFAULT fund_cd, channel COLLATE DATABASE_DEFAULT channel, sharia_indicator COLLATE DATABASE_DEFAULT sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness 
		from #etl5_temp_pruaman_existingbusiness 
		union all 
		select cast(policy_no as varchar(36)) COLLATE DATABASE_DEFAULT as policy_no, product_cd, benefit_cd, status, product_desc, fund_cd, channel, sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness 
		from #etl5_temp_pruamans_existingbusiness 
		union all 
		select policy_no, product_cd, benefit_cd, status, product_desc, fund_cd, channel, sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness 
		from #etl5_temp_paylife_existingbusiness 
		union all 
		select POLICY_NO , product_cd, benefit_cd, status, product_desc, fund_cd, channel, sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness 
		from #etl5_temp_creditshield_existingbusiness
		union all 
		select policy_no, product_cd, benefit_cd, status, product_desc, fund_cd, channel, sharia_indicator, driver_source, total_policy_per_product, goc, cp_agency_non_sharia_new_bussiness, cp_agency_sharia_new_bussiness, cp_bancassurance_non_sharia_new_bussiness, cp_bancassurance_sharia_new_bussiness, cp_dmtm_non_sharia_new_bussiness, cp_dmtm_sharia_new_bussiness, cp_agency_non_sharia_existing_bussiness, cp_agency_sharia_existing_bussiness, cp_bancassurance_non_sharia_existing_bussiness, cp_bancassurance_sharia_existing_bussiness, cp_dmtm_non_sharia_existing_bussiness, cp_dmtm_sharia_existing_bussiness 
		from #etl5_temp_pruemas_existingbusiness
		
	';
	--Drop Table
	EXEC (@sql_dropTEB);
	--Create Dynamic table for 
	EXEC( @sql_createTEB+@sql_selectTEB);
	
	print 'create '+@V_TNAME_EB
	 
	
	--Drop Table
--	EXEC (@sql_dropTNB);
--	EXEC (@sql_dropTEB);
	
	IF OBJECT_ID('tempdb..#etl5_temp_lifeasia_newbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_lifeasia_newbusiness
		END;
		--drop table #etl5_temp_lifeasia_newbusiness
		 	IF OBJECT_ID('tempdb..#etl5_temp_pruaman_newbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_pruaman_newbusiness
		END;
		--drop table #etl5_temp_pruaman_newbusiness
		 	IF OBJECT_ID('tempdb..#etl5_temp_pruamans_newbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_pruamans_newbusiness
		END;
		--drop table #etl5_temp_pruamans_newbusiness
		 IF OBJECT_ID('tempdb..#etl5_temp_paylife_newbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_paylife_newbusiness
		END;
		--drop table #etl5_temp_paylife_newbusiness
		 IF OBJECT_ID('tempdb..#etl5_temp_creditshield_newbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_creditshield_newbusiness
		END;
		--drop table #etl5_temp_creditshield_newbusiness
		 IF OBJECT_ID('tempdb..#etl5_temp_pruemas_newbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_pruemas_newbusiness
		END;
		--drop table #etl5_temp_pruemas_newbusiness
		 IF OBJECT_ID('tempdb..#etl5_temp_lifeasia_existingbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_lifeasia_existingbusiness
		END;
		--drop table #etl5_temp_lifeasia_existingbusiness
		 IF OBJECT_ID('tempdb..#etl5_temp_pruaman_existingbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_pruaman_existingbusiness
		END;
		--drop table #etl5_temp_pruaman_existingbusiness
		IF OBJECT_ID('tempdb..#etl5_temp_pruamans_existingbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_pruamans_existingbusiness
		END;
		--drop table #etl5_temp_pruamans_existingbusiness
		IF OBJECT_ID('tempdb..#etl5_temp_paylife_existingbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_paylife_existingbusiness
		END;
		--drop table #etl5_temp_paylife_existingbusiness
		IF OBJECT_ID('tempdb..#etl5_temp_creditshield_existingbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_creditshield_existingbusiness
		END;
		--drop table #etl5_temp_creditshield_existingbusiness
		IF OBJECT_ID('tempdb..#etl5_temp_pruemas_existingbusiness') IS NOT NULL
		BEGIN
			DROP TABLE #etl5_temp_pruemas_existingbusiness
		END;
		--drop table #etl5_temp_pruemas_existingbusiness 
		IF OBJECT_ID('tempdb..#stag_etl5_temp_total_policy_per_channel') IS NOT NULL
		BEGIN
			DROP TABLE #stag_etl5_temp_total_policy_per_channel
		END;
		
	--PRINT	'End : '+ cast(FORMAT(GETDATE(),'yyyy-MM-dd HH:mm') as varchar(30))
	END TRY

	BEGIN CATCH
		DECLARE @ErrorMessage NVARCHAR(4000);  
	    DECLARE @ErrorSeverity INT;  
	    DECLARE @ErrorState INT;  
	  
	    SELECT   
	        @ErrorMessage = ERROR_MESSAGE(),  
	        @ErrorSeverity = ERROR_SEVERITY(),  
	        @ErrorState = ERROR_STATE();  
	  
	    -- Use RAISERROR inside the CATCH block to return error  
	    -- information about the original error that caused  
	    -- execution to jump to the CATCH block.  
	    RAISERROR (@ErrorMessage, -- Message text.  
	               @ErrorSeverity, -- Severity.  
	               @ErrorState -- State.  
	               );  
	END CATCH
	
END;
