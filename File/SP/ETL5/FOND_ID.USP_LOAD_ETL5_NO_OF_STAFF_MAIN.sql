CREATE PROC [FOND_ID].[USP_LOAD_ETL5_NO_OF_STAFF_MAIN] @batch [varchar](12) AS 
--ex  @batch ='20190301'
BEGIN
--	DECLARE @batch [nvarchar](30)='20190101'
	DECLARE @batchdate_ nvarchar(10)=left(@batch,4)+'0'+SUBSTRING(@batch,5,2) 
--	select @batchdate_
	DECLARE @V_START		datetime;
	DECLARE @V_END			datetime;
	DECLARE @V_FUNCTION_NAME	NVARCHAR(2000) = 'FOND_ID.FOND_ETL5_NO_STAF_DRIVER_DETAIL';
	DECLARE @V_DESCRIPTION	NVARCHAR(2000);
	DECLARE @V_CMD			NVARCHAR(2000);
	DECLARE @V_SEQNO			integer = 0;
	DECLARE @V_PRD_ID		integer;
	DECLARE @V_CREATED_DATE	datetime;
	DECLARE @V_START_DATE	date;
	DECLARE @V_END_DATE		date;
	BEGIN TRY
		SET @V_START_DATE	= convert(date, cast(@batch as varchar(8))); -- valuation extract date
		PRINT	'Start date :' + convert(varchar,@V_START_DATE,112);
		SET @V_START 	= convert(datetime,getDATE());

		SET @V_DESCRIPTION 	= 'Start ' + @V_FUNCTION_NAME + ' : ' + convert(varchar,@V_START,121);
		PRINT	@V_DESCRIPTION;
		SET @V_SEQNO		= @V_SEQNO + 1;

		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
		
		---------------------------------EXEC SP1----------------------------------------	
	
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'EXEC FOND_ID.USP_LOAD_ETL5_NO_OF_STAFF_STEP1 : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
		
		exec FOND_ID.USP_LOAD_ETL5_NO_OF_STAFF_STEP1 @batchdate=@batchdate_;
	
		---------------------------------EXEC SP2----------------------------------------	
	
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'EXEC FOND_ID.USP_LOAD_ETL5_NO_OF_STAFF_STEP2 : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
		exec FOND_ID.USP_LOAD_ETL5_NO_OF_STAFF_STEP2 @batchdate=@batchdate_;
	    
		---------------------------------EXEC SP3----------------------------------------	
	
		BEGIN TRANSACTION;
		SET @V_SEQNO 	= @V_SEQNO + 1;
		SET @V_START 	= convert(datetime,getDATE());
		SET @V_DESCRIPTION	= 'EXEC FOND_ID.USP_LOAD_ETL5_NO_OF_STAFF_STEP3 : ' + convert(varchar,@V_START,121);
		PRINT @V_DESCRIPTION;
		
		INSERT into FOND_ID.FOND_IFRS17_PROC_LOG(PROC_DATE,FUNC_NAME,SEQNO,DESCRIPTION)
		VALUES (@V_START,@V_FUNCTION_NAME,@V_SEQNO,@V_DESCRIPTION);
	
		exec FOND_ID.USP_LOAD_ETL5_NO_OF_STAFF_STEP3 @batchdate=@batchdate_;
	
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
	
END CATCH
	
END

