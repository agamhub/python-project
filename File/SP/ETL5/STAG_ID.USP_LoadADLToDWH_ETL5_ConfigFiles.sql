CREATE PROC [STAG_ID].[USP_LoadADLToDWH_ETL5_ConfigFiles] @schemaName [NVARCHAR](100),@tableName [NVARCHAR](100),@batchId [NVARCHAR](100),@batchRunId [NVARCHAR](100),@jobId [NVARCHAR](100),@jobRunId [NVARCHAR](100),@batchDate [NVARCHAR](8),@blobURL [NVARCHAR](1000),@fileLocation [NVARCHAR](1000),@loadType [NVARCHAR](500),@delimiterFile [NVARCHAR](10),@firstRowFile [NVARCHAR](10),@columnOffset [INT] OUT AS BEGIN

DECLARE @crdID NVARCHAR(1000) = 'CRDID_' + @schemaName + @tableName;
DECLARE @extDS NVARCHAR(1000) = 'EXTDS_' + @schemaName + @tableName; 
DECLARE @extFF NVARCHAR(1000) = 'EXTFF_' + @schemaName + @tableName ;
DECLARE @extTB NVARCHAR(1000) = @schemaName + '.' + @tableName + '_EXTTB';
DECLARE @tgtTB NVARCHAR(1000);
SET @tgtTB= @schemaName + '.' + (select CASE WHEN RIGHT(@tableName,4) IN ('_IAC','_IAS') THEN  SUBSTRING(@tableName,1,CHARINDEX(RIGHT(@tableName,4), @tableName)-1) ELSE @tableName END AS YY); 

SET @blobURL = 'plai@stasgrassprdaz1veepo7pla.dfs.core.windows.net'

-- Drop temporary table Command
DECLARE @dropCrdIDSQL NVARCHAR(1000) = 'DROP DATABASE SCOPED CREDENTIAL ' + @crdID;
DECLARE @dropExtDSSQL NVARCHAR(1000) = 'DROP EXTERNAL DATA SOURCE ' + @extDS;
DECLARE @dropExtFFSQL NVARCHAR(1000) = 'DROP EXTERNAL FILE FORMAT ' + @extFF;
DECLARE @dropExtTBSQL NVARCHAR(1000) = 'DROP EXTERNAL TABLE ' + @extTB;

-- Create database scoped credential
DECLARE @crdIDSQL NVARCHAR(1000) = 'CREATE DATABASE SCOPED CREDENTIAL ' + @crdID + ' WITH IDENTITY = ''Managed Service Identity''';
PRINT @crdIDSQL;

-- Create external data source
DECLARE @extDSSQL NVARCHAR(1000) = 'CREATE EXTERNAL DATA SOURCE ' + @extDS
	+ ' WITH ( TYPE = HADOOP, LOCATION = ''abfss://' + @blobURL
	+ ''', CREDENTIAL = ' + @crdID
	+ ')';
PRINT @extDSSQL;

-- Create external file format
DECLARE @extFFSQL NVARCHAR(1000) = 'CREATE EXTERNAL FILE FORMAT ' + @extFF
	+ ' WITH ( FORMAT_TYPE = DELIMITEDTEXT,
		FORMAT_OPTIONS (
	        FIELD_TERMINATOR = '''+ @delimiterFile +''',
			STRING_DELIMITER = ''0x22'',
			FIRST_ROW = '+ @firstRowFile +',
			USE_TYPE_DEFAULT = FALSE,
			Encoding  = ''UTF8''
		))';
PRINT @extFFSQL +'';

BEGIN TRY
	EXECUTE sp_executesql @dropExtFFSQL;
END TRY
BEGIN CATCH
	PRINT 'FF does not exist'
END CATCH

BEGIN TRY
	EXECUTE sp_executesql @dropExtDSSQL;
END TRY
BEGIN CATCH
	PRINT 'DS does not exist'
END CATCH

BEGIN TRY
	EXECUTE sp_executesql @dropCrdIDSQL;
END TRY
BEGIN CATCH
	PRINT 'CRD does not exist'
END CATCH

BEGIN TRY
	EXECUTE sp_executesql @crdIDSQL;
	EXECUTE sp_executesql @extDSSQL;
	EXECUTE sp_executesql @extFFSQL;
END TRY
BEGIN CATCH
	PRINT 'Credential Already Exist Skipping..'
END CATCH

-- Create external table
DECLARE @noOfColumn INT;
DECLARE @createExtTableSQL NVARCHAR(MAX);
DECLARE @dropExtTableSQL NVARCHAR(MAX);
DECLARE @counter INT = 1;
DECLARE @counterColName INT = 1;
DECLARE @colName NVARCHAR(MAX);

SELECT @noOfColumn = COUNT(1) FROM sys.columns WHERE OBJECT_ID=OBJECT_ID(@tgtTB);
SET @colName=(select name FROM sys.columns where OBJECT_ID=OBJECT_ID(@tgtTB) and COLUMN_ID=1)
SET @createExtTableSQL = 'CREATE EXTERNAL TABLE ' + @extTB + '( ['+@colName+'] NVARCHAR(MAX)'
WHILE @counter < (@noOfColumn - 6)
BEGIN
	SET @counterColName=@counter+1
	SET @colName=(select name FROM sys.columns where OBJECT_ID=OBJECT_ID(@tgtTB) and COLUMN_ID=@counterColName)
	SET @createExtTableSQL = @createExtTableSQL + ',[' + @colName + '] NVARCHAR(MAX)'
	SET @counter = @counter + 1;
END
SET @createExtTableSQL = @createExtTableSQL 
	+ ') WITH ( LOCATION = ''' + @fileLocation
	+ ''', DATA_SOURCE = ' + @extDS
	+ ', FILE_FORMAT = ' + @extFF
	+ ', REJECT_TYPE = VALUE, REJECT_VALUE = 1 '
	+ ')'
PRINT @createExtTableSQL;

BEGIN TRY
	EXECUTE sp_executesql @dropExtTBSQL;
	PRINT 'External table exist, dropping table..'
END TRY
BEGIN CATCH
	PRINT 'External Table does not exists..'
END CATCH

EXECUTE sp_executesql @createExtTableSQL;

--IF (UPPER(@loadType) = 'TRUNCATE')
--BEGIN
--	DECLARE @truncateTgtSQL NVARCHAR(2000) = 'TRUNCATE TABLE ' + @tgtTB;
--	PRINT @truncateTgtSQL + ''
--	EXECUTE sp_executesql @truncateTgtSQL;
--END
--ELSE IF (UPPER(@loadType) = 'APPEND')
--BEGIN
--	DECLARE @deleteTgtSQL NVARCHAR(2000) = 'DELETE FROM ' + @tgtTB + ' WHERE BATCHDATE = ''' +@batchDate + '''';
--	PRINT @deleteTgtSQL + ''
--	EXECUTE sp_executesql @deleteTgtSQL;
--END
--ELSE
--BEGIN
--	PRINT ''
--END

-- Insert data into target table
--DECLARE @insertTgtTBSQL NVARCHAR(1000) = 'INSERT INTO ' + @tgtTB
--	+ ' SELECT *, ' + CAST (@batchId AS VARCHAR)
--	+ ', ' + CAST (@batchRunId AS VARCHAR)
--	+ ', ' + CAST (@jobId AS VARCHAR)
--	+ ', ' + CAST (@jobRunId AS VARCHAR)
--	+ ', ' + CAST (@batchDate AS VARCHAR)
--	+ ', GETDATE() FROM ' + @extTB;
--PRINT @insertTgtTBSQL + ''
--EXECUTE sp_executesql @insertTgtTBSQL;


-- Create Control file External table
-- BEGIN TRY
	-- EXECUTE sp_executesql @dropExtTBCtrlSQL;
	-- PRINT 'External control table exist, dropping table..'
-- END TRY
-- BEGIN CATCH
	-- PRINT 'External control table does not exists..'
-- END CATCH

--DECLARE @createCtrlExtTableSQL NVARCHAR(MAX);
--SET @counter = 1;

-- SET @createCtrlExtTableSQL = 'CREATE EXTERNAL TABLE ' + @extTBCtrl + '( column_0 NVARCHAR(MAX)'
-- WHILE @counter < (9)
-- BEGIN
	-- SET @createCtrlExtTableSQL = @createCtrlExtTableSQL + ', column_' + CAST(@counter AS VARCHAR) + ' NVARCHAR(MAX)'
	-- SET @counter = @counter + 1;
-- END
-- SET @createCtrlExtTableSQL = @createCtrlExtTableSQL 
	-- + ') WITH ( LOCATION = ''' + @fileLocationCtrl
	-- + ''', DATA_SOURCE = ' + @extDS
	-- + ', FILE_FORMAT = ' + @extFF
	-- + ')'
-- PRINT @createCtrlExtTableSQL;
-- EXECUTE sp_executesql @createCtrlExtTableSQL;

-- Delete existing record
--DECLARE @deleteCtrlExtTableSQL NVARCHAR(MAX);

-- Get date
--DECLARE @dayOfMonth NVARCHAR(2);
--SELECT @dayOfMonth = 
--	CASE 
--		WHEN LEN(@batchDate)=8 THEN SUBSTRING(@batchDate,7,2) 
--		ELSE '01'
--	END; 

-- set @deleteCtrlExtTableSQL = 'DELETE FROM ' + @tgtTBCtrl +' WHERE 
	-- STAG_JOB_MASTER_ID = '''+ @jobId +''' '
-- PRINT @deleteCtrlExtTableSQL;
-- EXECUTE sp_executesql @deleteCtrlExtTableSQL;

-- Insert Control File Data
-- DECLARE @insertCtrlExtTableSQL NVARCHAR(MAX);
-- set @insertCtrlExtTableSQL = 'INSERT INTO ' + @tgtTBCtrl +' 
		-- (CONTROL_FILE_ID
		-- ,FILE_NAME
		-- ,BATCH_DT
		-- ,SRC_FILE_GEN_DT
        -- ,SRC_FILE_GEN_TIME
        -- ,RECORD_COUNT
		-- ,[TOTAL_CR_AMT]
        -- ,[TOTAL_DR_AMT]
		-- ,[TRANSFER_MODE]
		-- ,[STAG_JOB_MASTER_ID]
		-- ,CURR_IND
		-- ,CTRL_FILE_NM)'
	-- + ' SELECT
			-- '''+ @jobId +'''
			-- ,SUBSTRING(SUBSTRING( '''+ @fileLocationCtrl +''' , LEN('''+ @fileLocationCtrl +''') -  CHARINDEX(''/'',REVERSE('''+ @fileLocationCtrl +''')) + 2  , LEN('''+ @fileLocationCtrl +''')  ), 1, 99) 
			-- ,CONVERT(DATETIME2,substring('''+ @batchDate +''',0,5) + substring('''+ @batchDate +''',5,2)+ ''' + @dayOfMonth + ''',112)
			-- ,convert(datetime2, substring(column_3,0,20), 120)
			-- ,convert(datetime2, substring(column_3,0,20), 120)
			-- ,column_4
			-- ,column_5
			-- ,column_6
			-- ,''S''
			-- ,'''+ @jobId +'''
			-- ,''Y''
			-- ,SUBSTRING(SUBSTRING( '''+ @fileLocationCtrl +''' , LEN('''+ @fileLocationCtrl +''') -  CHARINDEX(''/'',REVERSE('''+ @fileLocationCtrl +''')) + 2  , LEN('''+ @fileLocationCtrl +''')  ), 1, 99) 
			-- FROM ' + @extTBCtrl;
-- PRINT @insertCtrlExtTableSQL + ''
-- EXECUTE sp_executesql @insertCtrlExtTableSQL;
-- EXECUTE sp_executesql @dropExtTBCtrlSQL;

--EXECUTE sp_executesql @dropExtTBSQL;
--EXECUTE sp_executesql @dropExtDSSQL;
--EXECUTE sp_executesql @dropExtFFSQL;
--EXECUTE sp_executesql @dropCrdIDSQL;


END;;;

