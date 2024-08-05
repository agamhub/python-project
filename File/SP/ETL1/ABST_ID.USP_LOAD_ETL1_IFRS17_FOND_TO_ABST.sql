CREATE PROC [ABST_ID].[USP_LOAD_ETL1_IFRS17_FOND_TO_ABST] @batch_run_id [int],@job_run_id [int],@batchdate [nvarchar](20),@spname [nvarchar](255) AS

DECLARE @sql [NVARCHAR] (MAX);

SET @sql = N'EXEC '+@spname + ' ' + CAST(@batch_run_id AS VARCHAR(100)) + ' ,' +  CAST(@job_run_id AS VARCHAR(100)) + ', ''' + @batchdate + '''';

PRINT @sql;

EXEC sp_executesql @sql;






