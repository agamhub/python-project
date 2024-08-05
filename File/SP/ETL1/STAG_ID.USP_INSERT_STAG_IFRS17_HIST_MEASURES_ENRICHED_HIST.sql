CREATE PROC [STAG_ID].[USP_INSERT_STAG_IFRS17_HIST_MEASURES_ENRICHED_HIST] @account [varchar](50),@container [varchar](15),@filename [varchar](100),@stagingschema [varchar](100),@tablename [varchar](100) AS 
BEGIN

DECLARE @location nvarchar(500)
SET @location='abfss://'+@container+'@'+@account+'.dfs.core.windows.net'

set @container=replace(@container,'-','_')


DECLARE @CREATEEXTDATASOURCE nvarchar(1000);
DECLARE @CREATEEXTTABLE nvarchar(4000);
DECLARE @DROPEXTTABLE nvarchar(1000);
DECLARE @INSERTTABLE nvarchar(4000);

declare @extTBL nvarchar(500) ='IF EXISTS (Select * from sys.external_tables
where schema_id = (Select schema_id from sys.schemas where name ='''+ @stagingschema+''')
and name = ''EXT_STAG_IFRS17_HIST_MEASURES_ENRICHED_HIST'')
DROP EXTERNAL TABLE '+@stagingschema +'.EXT_STAG_IFRS17_HIST_MEASURES_ENRICHED_HIST'
exec(@extTBL)

declare @extDS nvarchar(500) = 'IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name=''ADLS_REG_ID_ENRICHED_HIST_'+@container+''')
CREATE EXTERNAL DATA SOURCE ADLS_REG_ID_ENRICHED_HIST_'+@container+'    
WITH (TYPE = HADOOP, LOCATION = '''+@location+''', CREDENTIAL = [msi_cred])'
EXEC (@extDS)

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name='ADLS_REG_DELIMITEDTEXT_2_ENRICHED_HIST')
CREATE EXTERNAL FILE FORMAT [ADLS_REG_DELIMITEDTEXT_2_ENRICHED_HIST] WITH (FORMAT_TYPE = DELIMITEDTEXT, FORMAT_OPTIONS (FIELD_TERMINATOR = N',', DATE_FORMAT = N'yyyy-MM-dd HH:mm:ss.fff', FIRST_ROW = 2, USE_TYPE_DEFAULT = False))

SET @CREATEEXTTABLE='CREATE EXTERNAL TABLE ' + @stagingschema + '.EXT_STAG_IFRS17_HIST_MEASURES_ENRICHED_HIST (
	Column0 varchar(300),
	Column1 varchar(300),
	Column2 varchar(300),
	Column3 varchar(300),
	Column4 varchar(300),
	Column5 varchar(300),
	Column6 varchar(300),
	Column7 varchar(300),
	Column8 varchar(300),
	Column9 varchar(300),
	Column10 varchar(300),
	Column11 varchar(300),
	Column12 varchar(300),
	Column13 varchar(300),
	Column14 varchar(300),
	Column15 varchar(300),
	Column16 varchar(300),
	Column17 varchar(300),
	Column18 varchar(300),
	Column19 varchar(300),
	Column20 varchar(300),
	Column21 varchar(300),
	Column22 varchar(300),
	Column23 varchar(300),
	Column24 varchar(300),
	Column25 varchar(300),
	Column26 varchar(300),
	Column27 varchar(300),
	Column28 varchar(300),
	Column29 varchar(300),
	Column30 varchar(300),
	Column31 varchar(300)
) WITH (
	LOCATION = ' + ''''+ @filename +'''' +  ',
    DATA_SOURCE = [ADLS_REG_ID_ENRICHED_HIST_' +@container +'],
    FILE_FORMAT = [ADLS_REG_DELIMITEDTEXT_2_ENRICHED_HIST]
)'


EXEC sp_executesql @CREATEEXTTABLE

SET @INSERTTABLE='INSERT INTO ' + @stagingschema + '.' + @tablename + ' (
	TABLEID,
	PROJECTID,
	LOADID,
	WORKGROUP,
	ENTITY,
	CONFIGURATIONSET,
	REPORTINGDATE,
	INSURANCECONTRACTSUBGROUPID,
	INSURANCECONTRACTPORTFOLIOID,
	POSTINGLEVEL,
	MEASUREMENTMETHOD,
	TRANSITIONAPPROACH,
	FUNCTIONALCURRENCY,
	COHORTID,
	CSMVARIABLENAME,
	DISCLOSURENAME,
	ACCOUNTINGEVENTTYPEID,
	ACCOUNTINGEVENTLEVEL1,
	ACCOUNTINGEVENTLEVEL2,
	COACODE,
	COAUDCODE,
	SLAMVARIABLEAMOUNT,
	CEDEDFLAG,
	HORIZONDATE,
	AMOUNT,
	PROCESSDTTM,
	LCODE01,
	LCODE02,
	GHOCUSTCDENTITY,
	GHOCUSTCDLOBT,
	GHOCUSTCDI17LOBT,
	PRODUCTID
	)
	SELECT
	case when Column0='''' then null else Column0 end Column0,
	case when Column1='''' then null else Column1 end Column1,
	case when Column2='''' then null else Column2 end Column2,
	case when Column3='''' then null else Column3 end Column3,
	case when Column4='''' then null else Column4 end Column4,
	case when Column5='''' then null else Column5 end Column5,
	--case when Column6='''' then null else CONVERT(DATE,Column6,101) end Column6,
	case when Column6='''' then null else CONVERT(DATE,Column6,23) end Column6,
	case when Column7='''' then null else Column7 end Column7,
	case when Column8='''' then null else Column8 end Column8,
	case when Column9='''' then null else Column9 end Column9,
	case when Column10='''' then null else Column10 end Column10,
	case when Column11='''' then null else Column11 end Column11,
	case when Column12='''' then null else Column12 end Column12,
	case when Column13='''' then null else Column13 end Column13,
	case when Column14='''' then null else Column14 end Column14,
	case when Column15='''' then null else Column15 end Column15,
	case when Column16='''' then null else Column16 end Column16,
	case when Column17='''' then null else Column17 end Column17,
	case when Column18='''' then null else Column18 end Column18,
	case when Column19='''' then null else Column19 end Column19,
	case when Column20='''' then null else Column20 end Column20,
	case when Column21='''' then null else Column21 end Column21,
	case when Column22='''' then null else Column22 end Column22,
	--case when Column23='''' then null else CONVERT(DATE,Column23,101) end Column23,
	case when Column23='''' then null else CONVERT(DATE,Column23,23) end Column23,
	case when Column24='''' then null else Column24 end Column24,
	--case when Column25='''' then null else CONVERT(DATE,Column25,101) end Column25,
	case when Column25='''' then null else CONVERT(DATETIME,Column25,102) end Column25,
	case when Column26='''' then null else Column26 end Column26,
	case when Column27='''' then null else Column27 end Column27,
	case when Column28='''' then null else Column28 end Column28,
	case when Column29='''' then null else Column29 end Column29,
	case when Column30='''' then null else Column30 end Column30,
	case when Column31='''' then null else Column31 end Column31
	FROM ' + @stagingschema + '.EXT_STAG_IFRS17_HIST_MEASURES_ENRICHED_HIST'

EXEC sp_executesql @INSERTTABLE

SET @DROPEXTTABLE= 'DROP EXTERNAL TABLE '+  @stagingschema + '.EXT_STAG_IFRS17_HIST_MEASURES_ENRICHED_HIST'
EXEC sp_executesql @DROPEXTTABLE



END

