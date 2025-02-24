SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [shadow].[BatchDeleteOneTableForPmsInstanceId](@PmsInstanceId uniqueidentifier, @ShadowTableName VARCHAR(30)) 
AS
	BEGIN
		-- Inspired by https://www.brentozar.com/archive/2018/04/how-to-delete-just-some-rows-from-a-really-big-table/ and 
		-- https://www.brentozar.com/archive/2021/06/lock-escalation-sucks-on-columnstore-indexes/
		SET NOCOUNT ON;
		SET DEADLOCK_PRIORITY LOW;
		DECLARE @BATCHSIZE INT = 1000;
		DECLARE @ITERATION INT
		DECLARE @ROWSDELETED INT
		DECLARE @TOTALROWS INT
		DECLARE @DELETE_SQL VARCHAR(MAX)
		DECLARE @INSERT_DELTA_SQL VARCHAR(MAX)
		DECLARE @MSG VARCHAR(500)
		DECLARE @INSERT_SQL NVARCHAR(MAX);
		DECLARE @COPY_CLEARED_SQL NVARCHAR(MAX);
	
		SET @DELETE_SQL = ' ;WITH ToBeDeleted_CTE AS (
			SELECT TOP(' + CAST(@BATCHSIZE as varchar(10)) + ') PmsInstanceId 
			FROM ' + @ShadowTableName + ' 
			WHERE PmsInstanceId = ''' + CAST(@PmsInstanceId as varchar(36)) + ''') ' +
			'DELETE FROM ToBeDeleted_CTE WHERE PmsInstanceId=''' + CAST(@PmsInstanceId as varchar(36)) + ''';';
	
		SET @INSERT_DELTA_SQL = ' ;WITH ToBeDeleted_CTE AS (
			SELECT TOP(' + CAST(@BATCHSIZE AS VARCHAR(10)) + ') PmsInstanceId
			FROM ' + @ShadowTableName + '
			WHERE PmsInstanceId = ''' + CAST(@PmsInstanceId AS VARCHAR(36)) + ''')
			INSERT INTO ' + @shadowTableName + 'Delta
			(PmsInstanceId, UtcCreatedDate, ActionPerformed)
			SELECT PmsInstanceId, GETUTCDATE(), 3
			FROM ToBeDeleted_CTE;'
	
		SET @COPY_CLEARED_SQL = 
		'INSERT INTO [shadow].[AvailabilitySlotCleared] (PmsInstanceId,PmsLocationId,PmsProviderId,PmsResourceId,PmsAltLocationId,StartDateTime,Length,SlotType,SwimLanes,SlotInfo)
		 (SELECT TOP(' + CAST(@BATCHSIZE AS VARCHAR(10)) + ') PmsInstanceId,PmsLocationId,PmsProviderId,PmsResourceId,PmsAltLocationId,StartDateTime,Length,SlotType,SwimLanes,SlotInfo 
		 FROM [shadow].[AvailabilitySlot])';

		SET @ITERATION = 0 
		SET @TOTALROWS = 0 

		-- Create a CTE to the top @batchsize records (should be significantly less than 5k to reduce change of lock escalation)
		-- and delete from that "view". This appears to be the most performant day to delete large amount of data from a large in-use table while minimizing locking.
		
		WHILE 1=1 -- Keep deleting from CTE View until nothing left
		BEGIN 				
			BEGIN TRANSACTION  -- Each batch is a transaction to minimize locking and preventing large rollbacks if something goes wrong
			IF @ShadowTableName <> '[shadow].AvailabilitySlot'
			  EXEC (@INSERT_DELTA_SQL)
			ELSE
			  EXEC (@COPY_CLEARED_SQL)
			EXEC (@DELETE_SQL)
			SET @ROWSDELETED=@@ROWCOUNT
			COMMIT TRANSACTION
			SET @ITERATION=@ITERATION+1
			SET @TOTALROWS=@TOTALROWS+@ROWSDELETED
			SET @MSG = @ShadowTableName + ' Delete Iteration: ' + CAST(@ITERATION AS VARCHAR) + ' Total ' + @ShadowTableName + ' records deleted: ' + CAST(@TOTALROWS AS VARCHAR)
			RAISERROR (@MSG, 0, 1) WITH NOWAIT -- this is not an error but a way to give immediate status   
			IF @ROWSDELETED < @BATCHSIZE  
				BREAK
		END

		SET @MSG = 'DONE!'
		RAISERROR (@MSG, 0, 1) WITH NOWAIT
    
	END
GO

CREATE PROCEDURE [shadow].[sp_AppointmentBlockMarkDeleted]
	@paramUtcOffset int,
	@inputData [shadow].udtt_PmsId readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	MERGE INTO [shadow].AppointmentBlock as target
	USING @inputData as source
	on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsAppointmentId) = TRIM(source.PmsId)
	WHEN MATCHED THEN
	UPDATE 
	SET 
		Deleted  = 1,
		UtcLastModifiedDate = @utcDate
	OUTPUT   -- delta table
		source.PmsInstanceId,
		TRIM(source.PmsId),
		@utcDate,
		3 AS ActionPerformed
	INTO [shadow].[AppointmentBlockDelta](PmsInstanceId, PmsAppointmentId, UtcCreatedDate, ActionPerformed); 
    
END
GO	

CREATE PROCEDURE [shadow].[sp_AppointmentBlockUpsert]
	@paramUtcOffset int,
	@inputData shadow.udtt_AppointmentBlock readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	select 
		input.PmsInstanceId, 
		input.PmsAppointmentId,
		ISNULL(loc.PmsLocationId, '0') as PmsLocationId,
		ISNULL(prov.PmsProviderId, '0') as PmsProviderId,
		ISNULL(resc.PmsResourceId, '0') as PmsResourceId,
	input.StartDate, 
		input.StartTime,
		input.[Length],
		input.Deleted,
		input.UTCCreatedDate,
		input.UTCLastModifiedDate
	into #temp
	from @inputData input
	left outer join [shadow].Location loc  on TRIM(loc.PmsLocationId)  = TRIM(input.PmsLocationId) AND loc.PmsInstanceId  = input.PmsInstanceId
	left outer join [shadow].Provider prov on TRIM(prov.PmsProviderId) = TRIM(input.PmsProviderId) AND prov.PmsInstanceId = input.PmsInstanceId
	left outer join [shadow].Resource resc on TRIM(resc.PmsResourceId) = TRIM(input.PmsResourceId) AND resc.PmsInstanceId = input.PmsInstanceId

	MERGE INTO [shadow].[AppointmentBlock] as target
	USING #temp as source
    ON target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsAppointmentId) = TRIM(source.PmsAppointmentId)
	WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (
				PmsInstanceId, 
				PmsAppointmentId, 
				PmsLocationId, 
				PmsProviderId, 
				PmsResourceId, 
				StartDate, 
				StartTime, 
				[Length],  
				Deleted, 
				UtcCreatedDate, 
				UtcLastModifiedDate)
		VALUES (
				source.PmsInstanceId, 
				TRIM(source.PmsAppointmentId), 
				ISNULL(TRIM(source.PmsLocationId),0), 
				ISNULL(TRIM(source.PmsProviderId),0), 
				ISNULL(TRIM(source.PmsResourceId),0), 
				source.StartDate, 
				source.StartTime, 
				source.[Length], 
				source.Deleted,  
				@utcDate, 
				@utcDate )
	WHEN MATCHED AND
	(
		CHECKSUM
		(
			ISNULL(CONVERT(VARCHAR(MAX),source.[Length]),CONVERT(VARCHAR(MAX),''))	+
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsLocationId,0)) +
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsProviderId,0)) +
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsResourceId,0))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.StartDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.StartTime),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.Deleted),CONVERT(VARCHAR(MAX),''))
		)
		<>
		CHECKSUM
		(
			ISNULL(CONVERT(VARCHAR(MAX),target.[Length]),CONVERT(VARCHAR(MAX),''))	+
			CONVERT(VARCHAR(MAX), target.PmsLocationId) +
			CONVERT(VARCHAR(MAX), target.PmsProviderId) +
			CONVERT(VARCHAR(MAX), target.PmsResourceId)	+
			ISNULL(CONVERT(VARCHAR(MAX),target.StartDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.StartTime),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.Deleted),CONVERT(VARCHAR(MAX),''))
		)
	)
	THEN
		UPDATE SET 
					target.[Length]		 = source.Length, 
					target.PmsLocationId = ISNULL(TRIM(source.PmsLocationId),0), 
					target.PmsProviderId = ISNULL(TRIM(source.PmsProviderId),0), 
					target.PmsResourceId = ISNULL(TRIM(source.PmsResourceId),0), 
					target.StartDate	 = source.StartDate, 
					target.StartTime	 = source.StartTime, 
					target.Deleted	     = source.Deleted, 
					UtcLastModifiedDate  = @utcDate
		OUTPUT   -- delta table
			source.PmsInstanceId,
			TRIM(source.PmsAppointmentId),
			@utcDate,
			CASE 
				WHEN $action = 'INSERT' THEN 1 
				WHEN $action = 'UPDATE' THEN 2
			 END AS ActionPerformed
		INTO [shadow].[AppointmentBlockDelta](PmsInstanceId, PmsAppointmentId, UtcCreatedDate, ActionPerformed); 
    
	drop table #temp

	SET NOCOUNT OFF;
END;
GO

CREATE PROCEDURE [shadow].[sp_AppointmentMarkDeleted]
		@paramUtcOffset int,
		@inputData shadow.udtt_PmsId readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	MERGE INTO [shadow].Appointment as target
	USING @inputData as source
	on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsAppointmentId) = TRIM(source.PmsId)
	WHEN MATCHED THEN
		UPDATE 
		SET 
			Deleted  = 1,
			UtcLastModifiedDate = @utcDate
		OUTPUT   -- delta table
			source.PmsInstanceId,
			TRIM(source.PmsId),
			@utcDate,
			3 AS ActionPerformed
		INTO [shadow].[AppointmentDelta](PmsInstanceId, PmsAppointmentId, UtcCreatedDate, ActionPerformed); 

END
GO

CREATE PROCEDURE [shadow].[sp_AppointmentReasonUpsert]
	@paramUtcOffset int,
	@nameOnly nvarchar(5),
	@inputData shadow.udtt_AppointmentReason readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	if @nameOnly = 'Y'
	BEGIN
		MERGE INTO [shadow].[AppointmentReason] as target
		USING @inputData as source
		on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.ReasonName) = TRIM(source.ReasonName)
		WHEN NOT MATCHED BY TARGET
		THEN
			INSERT (PmsInstanceId, PmsReasonId, ReasonName, UtcCreatedDate, UtcLastModifiedDate)
			VALUES (source.PmsInstanceId, TRIM(source.PmsReasonId), TRIM(source.ReasonName), @utcDate, @utcDate)
		WHEN MATCHED AND
		(
			CHECKSUM
			(
				TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.PmsReasonId),CONVERT(VARCHAR(MAX),'')))
			)
			<>
			CHECKSUM
			(
				TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.PmsReasonId),CONVERT(VARCHAR(MAX),'')))	
			)
		)
		THEN
			UPDATE SET 
				target.PmsReasonId = TRIM(source.PmsReasonId),
				target.ReasonName = TRIM(source.ReasonName), 
				UtcLastModifiedDate = @utcDate
			OUTPUT   -- delta table
				source.PmsInstanceId,
				TRIM(source.PmsReasonId),
				@utcDate,
				CASE 
				WHEN $action = 'INSERT' THEN 1 
				WHEN $action = 'UPDATE' THEN 2
				 END AS ActionPerformed
			INTO [shadow].[AppointmentReasonDelta](PmsInstanceId, PmsReasonId, UtcCreatedDate, ActionPerformed); 
	END
	ELSE
	BEGIN
		MERGE INTO [shadow].[AppointmentReason] as target
		USING @inputData as source
		on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsReasonId) = TRIM(source.PmsReasonId)
		WHEN NOT MATCHED BY TARGET
		THEN
			INSERT (PmsInstanceId, PmsReasonId, ReasonName, UtcCreatedDate, UtcLastModifiedDate)
			VALUES (source.PmsInstanceId, TRIM(source.PmsReasonId), TRIM(source.ReasonName), @utcDate, @utcDate)
		WHEN MATCHED AND
		(
			CHECKSUM
			(
				TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ReasonName),CONVERT(VARCHAR(MAX),'')))
			)
			<>
			CHECKSUM
			(
				TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ReasonName),CONVERT(VARCHAR(MAX),'')))
			)
		)
		THEN
			UPDATE SET 
				target.ReasonName = TRIM(source.ReasonName), 
				UtcLastModifiedDate = @utcDate
			OUTPUT   -- delta table
				source.PmsInstanceId,
				TRIM(source.PmsReasonId),
				@utcDate,
				CASE 
					WHEN $action = 'INSERT' THEN 1 
					WHEN $action = 'UPDATE' THEN 2
				END AS ActionPerformed
			 INTO [shadow].[AppointmentReasonDelta](PmsInstanceId, PmsReasonId, UtcCreatedDate, ActionPerformed); 
	END
 
	SET NOCOUNT OFF;
END;
GO

CREATE PROCEDURE [shadow].[sp_AppointmentUpsert]
	@paramUtcOffset int,
	@inputData shadow.udtt_Appointment readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	select 
		input.PmsInstanceId,
		input.PmsAppointmentId,
		ISNULL(pat.PmsPatientId, '0')   as PmsPatientId,
		ISNULL(loc.PmsLocationId, '0')  as PmsLocationId,
		ISNULL(prov.PmsProviderId, '0') as PmsProviderId,
		input.PmsResourceId,
		CASE WHEN (TRIM(input.PmsReasonId) is null or TRIM(input.PmsReasonId)='') 
			THEN 
				(SELECT PmsReasonId FROM [shadow].AppointmentReason ren WHERE TRIM(ren.ReasonName) = TRIM(input.ReasonName) and ren.PmsInstanceId = input.PmsInstanceId) 						
			ELSE 						
				(SELECT PmsReasonId FROM [shadow].AppointmentReason res WHERE TRIM(res.PmsReasonId) = TRIM(input.PmsReasonId) and res.PmsInstanceId = input.PmsInstanceId)
			END AS PmsReasonId,
		input.StartDate,
		input.StartTime,
		input.[Length],
		input.ApptType,
		input.ApptStatus,
		input.ShowedUp,
		input.SpendAmt,
		input.ICD1,
		input.ICD2,
		input.ICD3,
		input.CPT1,
		input.CPT2,
		input.CPT3,
		input.Deleted,
		input.PmsCreatedDate,
		input.PmsLastModifiedDate,
		input.UTCCreatedDate,
		input.UTCLastModifiedDate
	into #temp
	from @inputData input
	left outer join [shadow].Location loc  on TRIM(loc.PmsLocationId)  = TRIM(input.PmsLocationId) AND loc.PmsInstanceId = input.PmsInstanceId
	left outer join [shadow].Provider prov on TRIM(prov.PmsProviderId) = TRIM(input.PmsProviderId) AND prov.PmsInstanceId = input.PmsInstanceId
	left outer join [shadow].Patient  pat  on TRIM(pat.PmsPatientId)   = TRIM(input.PmsPatientId)  AND pat.PmsInstanceId = input.PmsInstanceId
	left outer join [shadow].Resource res  on TRIM(res.PmsResourceId)  = TRIM(input.PmsResourceId) AND res.PmsInstanceId = input.PmsInstanceId


	MERGE INTO [shadow].[Appointment] as target
	USING #temp as source
    ON target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsAppointmentId) = TRIM(source.PmsAppointmentId)
	WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (PmsInstanceId, 
				PmsAppointmentId, 
				PmsPatientId,
				PmsLocationId,
				PmsProviderId,
				PmsResourceId,
				PmsReasonId,
				StartDate,
				StartTime,
				[Length],
				ApptType,
				ApptStatus,
				ShowedUp,
				SpendAmt,
				ICD1,
				ICD2,
				ICD3,
				CPT1,
				CPT2,
				CPT3,
				Deleted,
				PmsCreatedDate,
				PmsLastModifiedDate,
				UTCCreatedDate, 
				UTCLastModifiedDate)
		VALUES (source.PmsInstanceId, 
				source.PmsAppointmentId, 
				ISNULL(source.PmsPatientId,0),
				ISNULL(source.PmsLocationId,0),
				ISNULL(source.PmsProviderId,0),
				ISNULL(source.PmsResourceId,0),
				ISNULL(source.PmsReasonId,0),
				source.StartDate,
				source.StartTime,
				source.[Length],
				source.ApptType,
				source.ApptStatus,
				source.ShowedUp,
				source.SpendAmt,
				source.ICD1,
				source.ICD2,
				source.ICD3,
				source.CPT1,
				source.CPT2,
				source.CPT3,
				source.Deleted,
				source.PmsCreatedDate,
				source.PmsLastModifiedDate,
				source.UTCCreatedDate, 
				source.UTCLastModifiedDate)
	WHEN MATCHED AND
	(  -- determine if anything changed
		CHECKSUM
		(
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsPatientId,0)) +
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsLocationId,0))	+
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsProviderId,0)) +
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsResourceId,0)) +
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsReasonId,0)) +
			ISNULL(CONVERT(VARCHAR(MAX),source.StartDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.StartTime),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.[Length]),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ApptType),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ApptStatus),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.ShowedUp),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.SpendAmt),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ICD1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ICD2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ICD3),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.CPT1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.CPT2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.CPT3),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.Deleted),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PmsCreatedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PmsLastModifiedDate),CONVERT(VARCHAR(MAX),''))
		)
		<>
		CHECKSUM
		(
			CONVERT(VARCHAR(MAX), target.PmsPatientId) +
			CONVERT(VARCHAR(MAX), target.PmsLocationId)	+
			CONVERT(VARCHAR(MAX), target.PmsProviderId) +
			CONVERT(VARCHAR(MAX), target.PmsResourceId) +
			CONVERT(VARCHAR(MAX), target.PmsReasonId) +
			ISNULL(CONVERT(VARCHAR(MAX),target.StartDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.StartTime),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.[Length]),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ApptType),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ApptStatus),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.ShowedUp),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.SpendAmt),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ICD1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ICD2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ICD3),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.CPT1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.CPT2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.CPT3),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.Deleted),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PmsCreatedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PmsLastModifiedDate),CONVERT(VARCHAR(MAX),''))
		)
	)
THEN    -- only update the columns that changed
    UPDATE SET 
        target.PmsPatientId        = CASE WHEN CHECKSUM(source.PmsPatientId)        <> CHECKSUM(target.PmsPatientId)        THEN ISNULL(source.PmsPatientId,0)  ELSE target.PmsPatientId END,
        target.PmsLocationId       = CASE WHEN CHECKSUM(source.PmsLocationId)       <> CHECKSUM(target.PmsLocationId)       THEN ISNULL(source.PmsLocationId,0) ELSE target.PmsLocationId END,
        target.PmsProviderId       = CASE WHEN CHECKSUM(source.PmsProviderId)       <> CHECKSUM(target.PmsProviderId)       THEN ISNULL(source.PmsProviderId,0) ELSE target.PmsProviderId END,
        target.PmsResourceId       = CASE WHEN CHECKSUM(source.PmsResourceId)       <> CHECKSUM(target.PmsResourceId)       THEN ISNULL(source.PmsResourceId,0) ELSE target.PmsResourceId END,
        target.PmsReasonId         = CASE WHEN CHECKSUM(source.PmsReasonId)         <> CHECKSUM(target.PmsReasonId)         THEN ISNULL(source.PmsReasonId,0)   ELSE target.PmsReasonId END,
        
		target.StartDate           = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), source.StartDate))	<> CHECKSUM(CONVERT(VARCHAR(MAX), target.StartDate))           
			THEN source.StartDate ELSE target.StartDate END,
        
		target.StartTime           = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), source.StartTime))    <> CHECKSUM(CONVERT(VARCHAR(MAX), target.StartTime))           
			THEN source.StartTime ELSE target.StartTime END,
        
		target.[Length]            = CASE WHEN CHECKSUM(source.[Length]) <> CHECKSUM(target.[Length]) THEN source.[Length] ELSE target.[Length] END,
        
		target.ApptType            = CASE WHEN CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ApptType))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), target.ApptType)))            
			THEN TRIM(source.ApptType) ELSE target.ApptType END,
        
		target.ApptStatus          = CASE WHEN CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ApptStatus))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), target.ApptStatus)))
			THEN TRIM(source.ApptStatus) ELSE target.ApptStatus END,
        
		target.ShowedUp            = CASE WHEN CHECKSUM(source.ShowedUp) <> CHECKSUM(target.ShowedUp) THEN source.ShowedUp   ELSE target.ShowedUp END,
        target.SpendAmt            = CASE WHEN CHECKSUM(source.SpendAmt) <> CHECKSUM(target.SpendAmt) THEN source.SpendAmt   ELSE target.SpendAmt END,
        
		target.ICD1                = CASE WHEN CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ICD1))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), target.ICD1))) 
			THEN TRIM(source.ICD1) ELSE target.ICD1 END,
        target.ICD2                = CASE WHEN CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ICD2))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), target.ICD2))) 
			THEN TRIM(source.ICD2) ELSE target.ICD2 END,
        target.ICD3                = CASE WHEN CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ICD3))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), target.ICD3))) 
			THEN TRIM(source.ICD3) ELSE target.ICD3 END,
        target.CPT1                = CASE WHEN CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.CPT1))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), target.CPT1))) 
			THEN TRIM(source.CPT1) ELSE target.CPT1 END,
        target.CPT2                = CASE WHEN CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.CPT2))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), target.CPT2))) 
			THEN TRIM(source.CPT2) ELSE target.CPT2 END,
			target.CPT3		   	  = CASE WHEN CHECKSUM(TRIM(CONVERT(VARCHAR(MAX),  source.CPT3))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), target.CPT3))) 
		THEN TRIM(source.CPT3) ELSE target.CPT3 END,

        target.Deleted             = CASE WHEN CHECKSUM(source.Deleted) <> CHECKSUM(target.Deleted) THEN source.Deleted ELSE target.Deleted END,
        
		target.PmsCreatedDate      = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), source.PmsCreatedDate)) <> CHECKSUM(CONVERT(VARCHAR(MAX), target.PmsCreatedDate))      
			THEN source.PmsCreatedDate ELSE target.PmsCreatedDate END,
        target.PmsLastModifiedDate = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), source.PmsLastModifiedDate)) <> CHECKSUM(CONVERT(VARCHAR(MAX), target.PmsLastModifiedDate)) 
			THEN source.PmsLastModifiedDate ELSE target.PmsLastModifiedDate END,
        target.UTCCreatedDate      = CASE WHEN CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.UTCCreatedDate)))      <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), target.UTCCreatedDate)))      
			THEN source.UTCCreatedDate ELSE target.UTCCreatedDate END,    
		target.UTCLastModifiedDate = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), source.UTCLastModifiedDate)) <> CHECKSUM(CONVERT(VARCHAR(MAX), target.UTCLastModifiedDate)) 
			THEN source.UTCLastModifiedDate ELSE target.UTCLastModifiedDate END
 -- output to delta table the values of the changed columns, or null if the columns didn't change
 -- using the CHECKSUM function is more efficient than comparing the actual string values
 -- the deleted identifier provides the old values 
 OUTPUT 
    source.PmsInstanceId,
    TRIM(source.PmsAppointmentId),
    
    CASE 
        WHEN $action = 'INSERT' THEN source.UTcCreatedDate
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), source.UTcCreatedDate)) <> CHECKSUM(CONVERT(VARCHAR(MAX), deleted.UTcCreatedDate))
            THEN source.UTcCreatedDate 
        ELSE NULL 
    END AS UTcCreatedDate,
    
    CASE 
        WHEN $action = 'INSERT' THEN 1 
        WHEN $action = 'UPDATE' THEN 2
    END AS ActionPerformed,
    
    CASE 
        WHEN $action = 'INSERT' THEN source.PmsPatientId
        WHEN $action = 'UPDATE' AND CHECKSUM(source.PmsPatientId) <> CHECKSUM(deleted.PmsPatientId) 
			THEN source.PmsPatientId  
        ELSE NULL 
    END AS PmsPatientId,
    
    CASE 
        WHEN $action = 'INSERT' THEN source.PmsLocationId
        WHEN $action = 'UPDATE' AND CHECKSUM(source.PmsLocationId) <> CHECKSUM(deleted.PmsLocationId) 
			THEN source.PmsLocationId 
        ELSE NULL 
    END AS PmsLocationId,
    
    CASE 
        WHEN $action = 'INSERT' THEN source.PmsProviderId
        WHEN $action = 'UPDATE' AND CHECKSUM(source.PmsProviderId) <> CHECKSUM(deleted.PmsProviderId) 
			THEN source.PmsProviderId 
        ELSE NULL 
    END AS PmsProviderId,
    
    CASE 
        WHEN $action = 'INSERT' THEN source.PmsResourceId
        WHEN $action = 'UPDATE' AND CHECKSUM(source.PmsResourceId) <> CHECKSUM(deleted.PmsResourceId) 
			THEN source.PmsResourceId 
        ELSE NULL 
    END AS PmsResourceId,
    
    CASE 
        WHEN $action = 'INSERT' THEN source.PmsReasonId
        WHEN $action = 'UPDATE' AND CHECKSUM(source.PmsReasonId) <> CHECKSUM(deleted.PmsReasonId) 
			THEN source.PmsReasonId   
        ELSE NULL 
    END AS PmsReasonId,
    
    CASE 
        WHEN $action = 'INSERT' THEN source.StartDate
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), source.StartDate)) <> CHECKSUM(CONVERT(VARCHAR(MAX), deleted.StartDate))           
            THEN source.StartDate           
        ELSE NULL 
    END AS StartDate,

    CASE 
        WHEN $action = 'INSERT' THEN source.StartTime
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), source.StartTime)) <> CHECKSUM(CONVERT(VARCHAR(MAX), deleted.StartTime))           
            THEN source.StartTime           
        ELSE NULL 
    END AS StartTime,
    
    CASE 
        WHEN $action = 'INSERT' THEN source.[Length]
        WHEN $action = 'UPDATE' AND CHECKSUM(source.[Length]) <> CHECKSUM(deleted.[Length]) 
			THEN source.[Length] 
        ELSE NULL 
    END AS [Length],
    
    CASE 
        WHEN $action = 'INSERT' THEN TRIM(source.ApptType)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ApptType))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.ApptType))) 
			THEN TRIM(source.ApptType) 
        ELSE NULL 
    END AS ApptType,
    
    CASE 
        WHEN $action = 'INSERT' THEN TRIM(source.ApptStatus)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ApptStatus))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.ApptStatus))) 
			THEN TRIM(source.ApptStatus) 
        ELSE NULL 
    END AS ApptStatus,

    CASE 
        WHEN $action = 'INSERT' THEN source.ShowedUp
        WHEN $action = 'UPDATE' AND CHECKSUM(source.ShowedUp) <> CHECKSUM(deleted.ShowedUp) 
			THEN source.ShowedUp 
        ELSE NULL 
    END AS ShowedUp,
    
    CASE 
        WHEN $action = 'INSERT' THEN source.SpendAmt
        WHEN $action = 'UPDATE' AND CHECKSUM(source.SpendAmt) <> CHECKSUM(deleted.SpendAmt) 
			THEN source.SpendAmt 
        ELSE NULL 
    END AS SpendAmt,

    CASE 
        WHEN $action = 'INSERT' THEN TRIM(source.ICD1)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ICD1))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.ICD1))) 
			THEN TRIM(source.ICD1) 
        ELSE NULL 
    END AS ICD1,
    
    CASE 
        WHEN $action = 'INSERT' THEN TRIM(source.ICD2)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ICD2))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.ICD2))) 
			THEN TRIM(source.ICD2) 
        ELSE NULL 
    END AS ICD2,
    
    CASE 
        WHEN $action = 'INSERT' THEN TRIM(source.ICD3)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ICD3))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.ICD3))) 
			THEN TRIM(source.ICD3) 
        ELSE NULL 
    END AS ICD3,
    
    CASE 
        WHEN $action = 'INSERT' THEN TRIM(source.CPT1)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.CPT1))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.CPT1))) 
			THEN TRIM(source.CPT1) 
        ELSE NULL 
    END AS CPT1,
    
    CASE 
        WHEN $action = 'INSERT' THEN TRIM(source.CPT2)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.CPT2))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.CPT2))) 
			THEN TRIM(source.CPT2) 
        ELSE NULL 
    END AS CPT2,
    
    CASE 
        WHEN $action = 'INSERT' THEN TRIM(source.CPT3)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.CPT3))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.CPT3))) 
			THEN TRIM(source.CPT3) 
        ELSE NULL 
    END AS CPT3,
    
    CASE 
        WHEN $action = 'INSERT' THEN source.Deleted
        WHEN $action = 'UPDATE' AND CHECKSUM(source.Deleted) <> CHECKSUM(deleted.Deleted) 
			THEN source.Deleted 
        ELSE NULL 
    END AS Deleted,

    CASE 
        WHEN $action = 'INSERT' THEN source.PmsCreatedDate
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), source.PmsCreatedDate)) <> CHECKSUM(CONVERT(VARCHAR(MAX), deleted.PmsCreatedDate))      
            THEN source.PmsCreatedDate      
        ELSE NULL 
    END AS PmsCreatedDate,
    
    CASE 
        WHEN $action = 'INSERT' THEN source.PmsLastModifiedDate
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), source.PmsLastModifiedDate)) <> CHECKSUM(CONVERT(VARCHAR(MAX), deleted.PmsLastModifiedDate)) 
            THEN source.PmsLastModifiedDate 
        ELSE NULL 
    END AS PmsLastModifiedDate,
    
    CASE 
        WHEN $action = 'INSERT' THEN source.UTCLastModifiedDate
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), source.UTCLastModifiedDate)) <> CHECKSUM(CONVERT(VARCHAR(MAX), deleted.UTCLastModifiedDate)) 
            THEN source.UTCLastModifiedDate 
        ELSE NULL 
    END AS UTCLastModifiedDate INTO [shadow].[AppointmentDelta](PmsInstanceId, 
                                 PmsAppointmentId, 
                                 UTcCreatedDate, 
                                 ActionPerformed, 
                                 PmsPatientId, 
                                 PmsLocationId, 
                                 PmsProviderId, 
                                 PmsResourceId, 
                                 PmsReasonId, 
                                 StartDate, 
                                 StartTime, 
                                 [Length],
                                 ApptType, 
                                 ApptStatus, 
                                 ShowedUp, 
                                 SpendAmt, 
                                 ICD1, 
                                 ICD2, 
                                 ICD3, 
                                 CPT1, 
                                 CPT2, 
                                 CPT3, 
                                 Deleted, 
                                 PmsCreatedDate, 
                                 PmsLastModifiedDate,
                                 UTCLastModifiedDate)
	; -- terminate merge statement
	drop table #temp

	SET NOCOUNT OFF;
END;
GO

CREATE PROCEDURE [shadow].[sp_AvailabilitySlotCompare]
AS
BEGIN
    SET NOCOUNT ON;

    -- insert new records into AvailabilitySlotDelta with ActionPerformed = 1
	-- (rows from the AvailabilitySlot table that don't exist in the AvailabilitySlotCleared)
	INSERT INTO [shadow].[AvailabilitySlotDelta] (
        [PmsInstanceId],
		[PmsLocationId],
		[PmsProviderId],
		[PmsResourceId],
		[PmsAltLocationId],
		[StartDateTime],
		[Length],
		[SlotType],
		[SwimLanes],
		[SlotInfo],
		[PmsCreatedDate],
		[PmsLastModifiedDate],
		[UTCCreatedDate],
		[UTCLastModifiedDate],
        [ActionPerformed]
    )
    SELECT
        [PmsInstanceId],
		[PmsLocationId],
		[PmsProviderId],
		[PmsResourceId],
		[PmsAltLocationId],
		[StartDateTime],
		[Length],
		[SlotType],
		[SwimLanes],
		[SlotInfo],
		[PmsCreatedDate],
		[PmsLastModifiedDate],
		[UTCCreatedDate],
		[UTCLastModifiedDate],
        1
    FROM
        [shadow].[AvailabilitySlot] AS source
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM [shadow].[AvailabilitySlotCleared] AS target
            WHERE source.[PmsInstanceId] = target.[PmsInstanceId]
            AND	  source.[PmsLocationId] = target.[PmsLocationId]
            AND	  source.[PmsProviderId] = target.[PmsProviderId]
            AND	  source.[StartDateTime] = target.[StartDateTime]
            AND	  source.[Length]		 = target.[Length]
            AND	  source.[SwimLanes]	 = target.[SwimLanes]
        );

    -- insert modified records into AvailabilitySlotDelta with ActionPerformed = 2
	-- (rows that exist in both but were modified)
    INSERT INTO [shadow].[AvailabilitySlotDelta] (
        [PmsInstanceId],
		[PmsLocationId],
		[PmsProviderId],
		[PmsResourceId],
		[PmsAltLocationId],
		[StartDateTime],
		[Length],
		[SlotType],
		[SwimLanes],
		[SlotInfo],
		[PmsCreatedDate],
		[PmsLastModifiedDate],
		[UTCCreatedDate],
		[UTCLastModifiedDate],
        [ActionPerformed]
    )
    SELECT
        source.[PmsInstanceId],
		source.[PmsLocationId],
		source.[PmsProviderId],
		source.[PmsResourceId],
		source.[PmsAltLocationId],
		source.[StartDateTime],
		source.[Length],
		source.[SlotType],
		source.[SwimLanes],
		source.[SlotInfo],
		source.[PmsCreatedDate],
		source.[PmsLastModifiedDate],
		source.[UTCCreatedDate],
		source.[UTCLastModifiedDate],
        2
    FROM
        [shadow].[AvailabilitySlot] AS source
        INNER JOIN [shadow].[AvailabilitySlotCleared] AS target
            ON    source.[PmsInstanceId] = target.[PmsInstanceId]
            AND	  source.[PmsLocationId] = target.[PmsLocationId]
            AND	  source.[PmsProviderId] = target.[PmsProviderId]
            AND	  source.[StartDateTime] = target.[StartDateTime]
            AND	  source.[Length]		 = target.[Length]
            AND	  source.[SwimLanes]	 = target.[SwimLanes]
    WHERE
        CHECKSUM(
            TRIM(CONVERT(VARCHAR(MAX), source.PmsLocationId)) +
            TRIM(CONVERT(VARCHAR(MAX), source.PmsProviderId)) +
            TRIM(CONVERT(VARCHAR(MAX), source.PmsResourceId)) +
            TRIM(CONVERT(VARCHAR(MAX), source.PmsAltLocationId)) +
            TRIM(CONVERT(VARCHAR(MAX), source.StartDateTime)) +
            TRIM(CONVERT(VARCHAR(MAX), source.[Length])) +
            TRIM(CONVERT(VARCHAR(MAX), source.SlotType)) +
            TRIM(CONVERT(VARCHAR(MAX), source.SwimLanes)) +
            TRIM(CONVERT(VARCHAR(MAX), source.SlotInfo))
        ) <>
        CHECKSUM(
            TRIM(CONVERT(VARCHAR(MAX), target.PmsLocationId)) +
            TRIM(CONVERT(VARCHAR(MAX), target.PmsProviderId)) +
            TRIM(CONVERT(VARCHAR(MAX), target.PmsResourceId)) +
            TRIM(CONVERT(VARCHAR(MAX), target.PmsAltLocationId)) +
            TRIM(CONVERT(VARCHAR(MAX), target.StartDateTime)) +
            TRIM(CONVERT(VARCHAR(MAX), target.[Length])) +
            TRIM(CONVERT(VARCHAR(MAX), target.SlotType)) +
            TRIM(CONVERT(VARCHAR(MAX), target.SwimLanes)) +
            TRIM(CONVERT(VARCHAR(MAX), target.SlotInfo))
        );

    -- insert deleted records into AvailabilitySlotDelta with ActionPerformed = 3
	-- (rows that exist in Cleared but not in AvailabilitySlot table are inserted into delta table)
    INSERT INTO [shadow].[AvailabilitySlotDelta] (
        [PmsInstanceId],
		[PmsLocationId],
		[PmsProviderId],
		[PmsResourceId],
		[PmsAltLocationId],
		[StartDateTime],
		[Length],
		[SlotType],
		[SwimLanes],
		[SlotInfo],
		[PmsCreatedDate],
		[PmsLastModifiedDate],
		[UTCCreatedDate],
		[UTCLastModifiedDate],
        [ActionPerformed]
    )
    SELECT
        [PmsInstanceId],
		[PmsLocationId],
		[PmsProviderId],
		[PmsResourceId],
		[PmsAltLocationId],
		[StartDateTime],
		[Length],
		[SlotType],
		[SwimLanes],
		[SlotInfo],
		[PmsCreatedDate],
		[PmsLastModifiedDate],
		[UTCCreatedDate],
		[UTCLastModifiedDate],
        3
    FROM
        [shadow].[AvailabilitySlotCleared] AS source
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM [shadow].[AvailabilitySlot] AS target
            WHERE source.[PmsInstanceId] = target.[PmsInstanceId]
            AND	  source.[PmsLocationId] = target.[PmsLocationId]
            AND	  source.[PmsProviderId] = target.[PmsProviderId]
            AND	  source.[StartDateTime] = target.[StartDateTime]
            AND	  source.[Length]		 = target.[Length]
            AND	  source.[SwimLanes]	 = target.[SwimLanes]
        );

END
GO

CREATE PROCEDURE [shadow].[sp_AvailabilitySlotMarkDeleted]
	@inputData  shadow.udtt_AvailabilitySlotId readonly
AS
BEGIN
    SET NOCOUNT ON;

	-- can't this be accomplised with a MERGE?

    -- Declare a table variable to hold the values to be deleted (and subsequently inserted into AvailabilitySlotDelta)
    DECLARE @DeletedRows TABLE (
		[PmsInstanceId] [uniqueidentifier],
		[PmsLocationId] [varchar],
		[PmsProviderId] [varchar],
		[StartDateTime] [datetime],
		[Length] [int],
		[SwimLanes] [int],
        UTCCreatedDate DATETIME,
        ActionPerformed TINYINT
    );

    -- Populate the table variable with the values to be deleted
	INSERT INTO @DeletedRows (PmsInstanceId, PmsLocationId,PmsProviderId,StartDateTime,[Length],SwimLanes)
		SELECT av.PmsInstanceId, av.PmsLocationId,av.PmsProviderId,av.StartDateTime,av.[Length],av.SwimLanes
		FROM [shadow].AvailabilitySlot av
		INNER JOIN @inputData id ON av.PmsInstanceId	= id.PmsInstanceId
								 AND av.PmsLocationId	= id.PmsLocationId
								 AND av.PmsProviderId	= id.PmsProviderId
								 AND av.[Length]		= id.[Length]
								 AND av.SwimLanes		= id.SwimLanes;
    
	-- Delete from AvailabilitySlot table
    DELETE av FROM [shadow].AvailabilitySlot av
    INNER JOIN @DeletedRows dr ON av.PmsInstanceId      = dr.PmsInstanceId  
								 AND av.PmsLocationId	= dr.PmsLocationId
								 AND av.PmsProviderId	= dr.PmsProviderId
								 AND av.[Length]		= dr.[Length]
								 AND av.SwimLanes		= dr.SwimLanes;
								 	
  -- Insert into AvailabilitySlotDelta table
    INSERT INTO [shadow].AvailabilitySlotDelta
        (PmsInstanceId, PmsLocationId,PmsProviderId,StartDateTime,[Length],SwimLanes, UTCCreatedDate, ActionPerformed)
    SELECT PmsInstanceId, TRIM(PmsLocationId),TRIM(PmsProviderId),StartDateTime,[Length],SwimLanes, GETUTCDATE(), 3
    FROM @DeletedRows;
END		
GO

CREATE PROCEDURE [shadow].[sp_InsurerUpsert]
	@paramUtcOffset int,
	@nameOnly nvarchar(5),
	@inputData shadow.udtt_Insurer readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	if @nameOnly = 'Y'
	BEGIN
		MERGE INTO [shadow].[Insurer] as target
		USING @inputData as source
		on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.InsurerName) = TRIM(source.InsurerName)
		WHEN NOT MATCHED BY TARGET
		THEN
			INSERT (PmsInstanceId, PmsInsurerId, InsurerName, UtcCreatedDate, UtcLastModifiedDate)
			VALUES (source.PmsInstanceId, TRIM(source.PmsInsurerId), TRIM(source.InsurerName), @utcDate, @utcDate)
		WHEN MATCHED AND
		(
			CHECKSUM
			(
				TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.PmsInsurerId),CONVERT(VARCHAR(MAX),'')))
			)
			<>
			CHECKSUM
			(
				TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.PmsInsurerId),CONVERT(VARCHAR(MAX),'')))
			)
		)
		THEN
			UPDATE SET 
				target.PmsInsurerId = TRIM(source.PmsInsurerId),
				target.InsurerName  = TRIM(source.InsurerName), 
				UtcLastModifiedDate = @utcDate
			OUTPUT   -- delta table
				source.PmsInstanceId,
				TRIM(source.PmsInsurerId),
				@utcDate,
				CASE 
				WHEN $action = 'INSERT' THEN 1 
				WHEN $action = 'UPDATE' THEN 2
				 END AS ActionPerformed
			INTO [shadow].[InsurerDelta](PmsInstanceId, PmsInsurerId, UtcCreatedDate, ActionPerformed); 
	END
	ELSE
	BEGIN
		MERGE INTO [shadow].[Insurer] as target
		USING @inputData as source
		on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsInsurerId) = TRIM(source.PmsInsurerId)
		WHEN NOT MATCHED BY TARGET
		THEN
			INSERT (PmsInstanceId, PmsInsurerId, InsurerName, UtcCreatedDate, UtcLastModifiedDate)
			VALUES (source.PmsInstanceId, TRIM(source.PmsInsurerId), TRIM(source.InsurerName), @utcDate, @utcDate)
		WHEN MATCHED AND
		(
			CHECKSUM
			(
				TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.InsurerName),CONVERT(VARCHAR(MAX),'')))
			)
			<>
			CHECKSUM
			(
				TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.InsurerName),CONVERT(VARCHAR(MAX),'')))
			)
		)
		THEN
			UPDATE SET 
				target.InsurerName = TRIM(source.InsurerName), 
				UtcLastModifiedDate = @utcDate
			OUTPUT   -- delta table
				source.PmsInstanceId,
				TRIM(source.PmsInsurerId),
				@utcDate,
				CASE 
				WHEN $action = 'INSERT' THEN 1 
				WHEN $action = 'UPDATE' THEN 2
				 END AS ActionPerformed
			INTO [shadow].[InsurerDelta](PmsInstanceId, PmsInsurerId, UtcCreatedDate, ActionPerformed); 
	END
 
	SET NOCOUNT OFF;
END;
GO

CREATE  PROCEDURE [shadow].[sp_LocationUpsert]
	@paramUtcOffset int,
	@inputData shadow.udtt_Location readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	MERGE INTO [shadow].[Location] as target
	USING @inputData as source
    ON target.PmsInstanceId = source.PmsInstanceId AND target.PmsLocationId = source.PmsLocationId
	WHEN NOT MATCHED BY TARGET
	THEN
        INSERT (PmsInstanceId, PmsLocationId, LocationName, TimeZone, UtcCreatedDate, UtcLastModifiedDate)
		VALUES (source.PmsInstanceId, source.PmsLocationId, TRIM(source.LocationName), TRIM(source.TimeZone), @utcDate, @utcDate)
	WHEN MATCHED AND
	(
		CHECKSUM
		(
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.LocationName),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.TimeZone),CONVERT(VARCHAR(MAX),'')))
		)
		<>
		CHECKSUM
		(
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.LocationName),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.TimeZone),CONVERT(VARCHAR(MAX),'')))
		)
	)
	THEN
		UPDATE SET 
            LocationName = TRIM(source.LocationName),
            TimeZone	 = TRIM(source.TimeZone),
            UtcLastModifiedDate = @utcDate
		OUTPUT   -- delta table
			source.PmsInstanceId,
			TRIM(source.PmsLocationId),
			@utcDate,
			CASE 
				WHEN $action = 'INSERT' THEN 1 
				WHEN $action = 'UPDATE' THEN 2
			 END AS ActionPerformed
		INTO [shadow].[LocationDelta](PmsInstanceId, PmsLocationId, UtcCreatedDate, ActionPerformed); 
    
	SET NOCOUNT OFF;
END;
GO

CREATE PROCEDURE [shadow].[sp_PatientMarkDeleted]
	@paramUtcOffset int,
	@inputData shadow.udtt_PmsId readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	MERGE INTO [shadow].Patient as target
	USING @inputData as source
	on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsPatientId) = TRIM(source.PmsId)
	WHEN MATCHED THEN
	UPDATE 
	SET 
		Deleted  = 1,
		UtcLastModifiedDate = @utcDate
	OUTPUT   -- delta table
		source.PmsInstanceId,
		TRIM(source.PmsId),
		@utcDate,
		3 AS ActionPerformed
	INTO [shadow].[PatientDelta](PmsInstanceId, PmsPatientId, UtcCreatedDate, ActionPerformed); 

END
GO

CREATE PROCEDURE [shadow].[sp_PatientUpsert]
    @paramUtcOffset int,
    @inputData shadow.udtt_Patient readonly
AS
BEGIN
    DECLARE @utcOffset int = @paramUtcOffset
    DECLARE @utcDate datetime = GetUTCDate()

    SET ANSI_WARNINGS OFF;
    SET NOCOUNT ON;

    SELECT 
        input.PmsInstanceId,input.PmsPatientId,ISNULL(loc.PmsLocationId, '0') as PmsLocationId,ISNULL(prov.PmsProviderId, '0') as PmsProviderId,input.PatientNumber,
        input.FirstName,input.LastName,input.PrimaryPhone,input.PrimaryPhoneType,input.SecondaryPhone,input.SecondaryPhoneType,input.CellPhone,input.Email,
        input.Address1,input.Address2,input.City,input.State,input.Country,input.Zip,input.DOB,input.SSN,input.Gender,input.PreferredLanguage,input.Title,
        input.CommunicationChoice1,input.CommunicationChoice2,input.CommunicationChoice3,input.SurveyEmailPreference,input.SurveySMSPreference,input.SurveyVoicePreference,input.SurveyPostalPreference,
        input.ReminderEmailPreference,input.ReminderSMSPreference,input.ReminderVoicePreference,input.ReminderPostalPreference,input.RecallEmailPreference,input.RecallSMSPreference,
        input.RecallVoicePreference,input.RecallPostalPreference,input.PickupEmailPreference,input.PickupSMSPreference,input.PickupVoicePreference,input.PickupPostalPreference,
        input.OtherEmailPreference,input.OtherSMSPreference,input.OtherVoicePreference,input.OtherPostalPreference,input.Status,input.ICD1,input.ICD2,input.ICD3,
		input.ICD1Date,input.ICD2Date,input.ICD3Date,
        CASE WHEN (TRIM(input.InsurerIDPrimary) IS NULL OR TRIM(input.InsurerIDPrimary) = '')
        THEN
            (SELECT PmsInsurerId FROM [shadow].Insurer ins1n WHERE TRIM(ins1n.InsurerName) = TRIM(input.InsurerNamePrimary) AND ins1n.PmsInstanceId = input.PmsInstanceId)
        ELSE
            (SELECT PmsInsurerId FROM [shadow].Insurer ins1 WHERE TRIM(ins1.PmsInsurerId) = TRIM(input.InsurerIDPrimary) AND ins1.PmsInstanceId = input.PmsInstanceId)
        END AS InsurerIDPrimary,
        CASE WHEN (TRIM(input.InsurerIDSecondary) IS NULL OR TRIM(input.InsurerIDSecondary) = '')
        THEN
            (SELECT PmsInsurerId FROM [shadow].Insurer ins2n WHERE TRIM(ins2n.InsurerName) = TRIM(input.InsurerNameSecondary) AND ins2n.PmsInstanceId = input.PmsInstanceId)
        ELSE
            (SELECT PmsInsurerId FROM [shadow].Insurer ins2 WHERE TRIM(ins2.PmsInsurerId) = TRIM(input.InsurerIDSecondary) AND ins2.PmsInstanceId = input.PmsInstanceId)
        END AS InsurerIDSecondary,
        CASE WHEN (TRIM(input.InsurerIDTertiary) IS NULL OR TRIM(input.InsurerIDTertiary) = '')
        THEN
            (SELECT PmsInsurerId FROM [shadow].Insurer ins3n WHERE TRIM(ins3n.InsurerName) = TRIM(input.InsurerNameTertiary) AND ins3n.PmsInstanceId = input.PmsInstanceId)
        ELSE
            (SELECT PmsInsurerId FROM [shadow].Insurer ins3 WHERE TRIM(ins3.PmsInsurerId) = TRIM(input.InsurerIDTertiary) AND ins3.PmsInstanceId = input.PmsInstanceId)
        END AS InsurerIDTertiary,    
        input.SpendingBehavior,input.ContactLensWearer,input.PrimaryInsuranceEIN,input.PrimaryInsuranceGroupId,input.SecondaryInsuranceGroupId,input.TertiaryInsuranceGroupId,
        input.PrimaryInsurancePolicyNo,input.SecondaryInsurancePolicyNo,input.TertiaryInsurancePolicyNo,input.PrimaryInsurancePlanName,input.SecondaryInsurancePlanName,
        input.TertiaryInsurancePlanName,input.PmsSpecificName1,input.PmsSpecificValue1,input.LastVisitOrExamDate,0 AS Deleted,input.PmsCreatedDate,input.PmsLastModifiedDate,
        input.UTCCreatedDate,input.UTCLastModifiedDate
    INTO #temp
    FROM @inputData input
    LEFT OUTER JOIN [shadow].Location loc  ON loc.PmsLocationId  = input.PmsLocationId AND loc.PmsInstanceId  = input.PmsInstanceId
    LEFT OUTER JOIN [shadow].Provider prov ON prov.PmsProviderId = input.PmsProviderId AND prov.PmsInstanceId = input.PmsInstanceId

    MERGE INTO Patient AS target
    USING #temp AS source
    ON target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsPatientId) = TRIM(source.PmsPatientId)
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (
            PmsInstanceId, PmsPatientId, PmsLocationId, PmsProviderId, PatientNumber, FirstName, LastName, 
            PrimaryPhone, PrimaryPhoneType, SecondaryPhone, SecondaryPhoneType, CellPhone, Email, 
            Address1, Address2, City, State, Country, Zip, DOB, SSN, Gender, PreferredLanguage, 
            Title, CommunicationChoice1, CommunicationChoice2, CommunicationChoice3, 
            SurveyEmailPreference, SurveySMSPreference, SurveyVoicePreference, SurveyPostalPreference, 
            ReminderEmailPreference, ReminderSMSPreference, ReminderVoicePreference, ReminderPostalPreference, 
            RecallEmailPreference, RecallSMSPreference, RecallVoicePreference, RecallPostalPreference, 
            PickupEmailPreference, PickupSMSPreference, PickupVoicePreference, PickupPostalPreference, 
            OtherEmailPreference, OtherSMSPreference, OtherVoicePreference, OtherPostalPreference, 
            Status, ICD1, ICD2, ICD3, ICD1Date, ICD2Date, ICD3Date, 
            InsurerIDPrimary, InsurerIDSecondary, InsurerIDTertiary, 
            SpendingBehavior, ContactLensWearer, PrimaryInsuranceEIN, 
            PrimaryInsuranceGroupId, SecondaryInsuranceGroupId, TertiaryInsuranceGroupId, 
            PrimaryInsurancePolicyNo, SecondaryInsurancePolicyNo, TertiaryInsurancePolicyNo, 
            PrimaryInsurancePlanName, SecondaryInsurancePlanName, TertiaryInsurancePlanName, 
            PmsSpecificName1, PmsSpecificValue1, LastVisitOrExamDate, Deleted, 
            PmsCreatedDate, PmsLastModifiedDate, UTCCreatedDate, UTCLastModifiedDate
        )
        VALUES (
            source.PmsInstanceId, TRIM(source.PmsPatientId), ISNULL(TRIM(source.PmsLocationId), 0), 
            ISNULL(TRIM(source.PmsProviderId), 0), TRIM(source.PatientNumber), TRIM(source.FirstName), 
            TRIM(source.LastName), TRIM(source.PrimaryPhone), TRIM(source.PrimaryPhoneType), 
            TRIM(source.SecondaryPhone), TRIM(source.SecondaryPhoneType), TRIM(source.CellPhone), 
            TRIM(source.Email), TRIM(source.Address1), TRIM(source.Address2), TRIM(source.City), 
            TRIM(source.State), TRIM(source.Country), TRIM(source.Zip), source.DOB, TRIM(source.SSN), 
            TRIM(source.Gender), TRIM(source.PreferredLanguage), TRIM(source.Title), 
            TRIM(source.CommunicationChoice1), TRIM(source.CommunicationChoice2), TRIM(source.CommunicationChoice3), 
            source.SurveyEmailPreference, source.SurveySMSPreference, source.SurveyVoicePreference, 
            source.SurveyPostalPreference, source.ReminderEmailPreference, source.ReminderSMSPreference, 
            source.ReminderVoicePreference, source.ReminderPostalPreference, source.RecallEmailPreference, 
            source.RecallSMSPreference, source.RecallVoicePreference, source.RecallPostalPreference, 
            source.PickupEmailPreference, source.PickupSMSPreference, source.PickupVoicePreference, 
            source.PickupPostalPreference, source.OtherEmailPreference, source.OtherSMSPreference, 
            source.OtherVoicePreference, source.OtherPostalPreference, source.Status, 
            TRIM(source.ICD1), TRIM(source.ICD2), TRIM(source.ICD3), source.ICD1Date, source.ICD2Date, 
            source.ICD3Date, ISNULL(source.InsurerIDPrimary, 0), ISNULL(source.InsurerIDSecondary, 0), 
            ISNULL(source.InsurerIDTertiary, 0), TRIM(source.SpendingBehavior), TRIM(source.ContactLensWearer), 
            TRIM(source.PrimaryInsuranceEIN), TRIM(source.PrimaryInsuranceGroupId), 
            TRIM(source.SecondaryInsuranceGroupId), TRIM(source.TertiaryInsuranceGroupId), 
            TRIM(source.PrimaryInsurancePolicyNo), TRIM(source.SecondaryInsurancePolicyNo), 
            TRIM(source.TertiaryInsurancePolicyNo), TRIM(source.PrimaryInsurancePlanName), 
            TRIM(source.SecondaryInsurancePlanName), TRIM(source.TertiaryInsurancePlanName), 
            TRIM(source.PmsSpecificName1), TRIM(source.PmsSpecificValue1), source.LastVisitOrExamDate, 
            source.Deleted, source.PmsCreatedDate, source.PmsLastModifiedDate, source.UTCCreatedDate, source.UTCLastModifiedDate
        )
WHEN MATCHED AND
(  -- determine if anything changed
		CHECKSUM
		(
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsLocationId,0))	+
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsProviderId,0)) +
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.PatientNumber),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.FirstName),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.LastName),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.PrimaryPhone),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.PrimaryPhoneType),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.SecondaryPhone),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.SecondaryPhoneType),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.CellPhone),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.Email),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.Address1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.Address2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.City),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.State),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.Country),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.Zip),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.DOB),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.SSN),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.Gender),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.PreferredLanguage),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.Title),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.CommunicationChoice1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.CommunicationChoice2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.CommunicationChoice3),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.SurveyEmailPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.SurveySMSPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.SurveyVoicePreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.SurveyPostalPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.ReminderEmailPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.ReminderSMSPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.ReminderVoicePreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.ReminderPostalPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.RecallEmailPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.RecallSMSPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.RecallVoicePreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.RecallPostalPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PickupEmailPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PickupSMSPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PickupVoicePreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PickupPostalPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.OtherEmailPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.OtherSMSPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.OtherVoicePreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.OtherPostalPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.Status),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ICD1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ICD2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ICD3),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.ICD1Date),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.ICD2Date),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.ICD3Date),CONVERT(VARCHAR(MAX),''))	+
			CONVERT(VARCHAR(MAX), ISNULL(source.InsurerIDPrimary,0)) +
			CONVERT(VARCHAR(MAX), ISNULL(source.InsurerIDSecondary,0)) +
			CONVERT(VARCHAR(MAX), ISNULL(source.InsurerIDTertiary,0)) +
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.SpendingBehavior),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),(CASE WHEN target.ContactLensWearer IS NOT NULL THEN target.ContactLensWearer ELSE source.ContactLensWearer END)),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.PrimaryInsuranceEIN),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.PrimaryInsuranceGroupId),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.SecondaryInsuranceGroupId),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.TertiaryInsuranceGroupId),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.PrimaryInsurancePolicyNo),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.SecondaryInsurancePolicyNo),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.TertiaryInsurancePolicyNo),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.PrimaryInsurancePlanName),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.SecondaryInsurancePlanName),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.TertiaryInsurancePlanName),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PmsSpecificName1),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PmsSpecificValue1),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),(CASE WHEN source.LastVisitOrExamDate > target.LastVisitOrExamDate THEN source.LastVisitOrExamDate ELSE target.LastVisitOrExamDate END)),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.Deleted),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PMSCreatedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PMSLastModifiedDate),CONVERT(VARCHAR(MAX),''))
		)
		<>
		CHECKSUM
		(
			CONVERT(VARCHAR(MAX), target.PmsLocationId)	+
			CONVERT(VARCHAR(MAX), target.PmsProviderId) +
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.PatientNumber),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.FirstName),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.LastName),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.PrimaryPhone),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.PrimaryPhoneType),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.SecondaryPhone),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.SecondaryPhoneType),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.CellPhone),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.Email),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.Address1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.Address2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.City),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.State),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.Country),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.Zip),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.DOB),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.SSN),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.Gender),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.PreferredLanguage),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.Title),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.CommunicationChoice1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.CommunicationChoice2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.CommunicationChoice3),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.SurveyEmailPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.SurveySMSPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.SurveyVoicePreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.SurveyPostalPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.ReminderEmailPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.ReminderSMSPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.ReminderVoicePreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.ReminderPostalPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.RecallEmailPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.RecallSMSPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.RecallVoicePreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.RecallPostalPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PickupEmailPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PickupSMSPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PickupVoicePreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PickupPostalPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.OtherEmailPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.OtherSMSPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.OtherVoicePreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.OtherPostalPreference),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.Status),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ICD1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ICD2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ICD3),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.ICD1Date),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.ICD2Date),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.ICD3Date),CONVERT(VARCHAR(MAX),''))	+
			CONVERT(VARCHAR(MAX), target.InsurerIDPrimary) +
			CONVERT(VARCHAR(MAX), target.InsurerIDSecondary) +
			CONVERT(VARCHAR(MAX), target.InsurerIDTertiary) +
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.SpendingBehavior),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ContactLensWearer),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.PrimaryInsuranceEIN),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.PrimaryInsuranceGroupId),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.SecondaryInsuranceGroupId),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.TertiaryInsuranceGroupId),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.PrimaryInsurancePolicyNo),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.SecondaryInsurancePolicyNo),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.TertiaryInsurancePolicyNo),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.PrimaryInsurancePlanName),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.SecondaryInsurancePlanName),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.TertiaryInsurancePlanName),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PmsSpecificName1),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PmsSpecificValue1),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.LastVisitOrExamDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.Deleted),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PMSCreatedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PMSLastModifiedDate),CONVERT(VARCHAR(MAX),''))
		)
)
THEN	-- only update the columns that changed
	UPDATE SET 
        target.PmsLocationId		= CASE WHEN CHECKSUM(source.PmsLocationId)		<> CHECKSUM(target.PmsLocationId)		
			THEN ISNULL(source.PmsLocationId,0) ELSE target.PmsLocationId END,
        target.PmsProviderId		= CASE WHEN CHECKSUM(source.PmsProviderId)		<> CHECKSUM(target.PmsProviderId)		
			THEN ISNULL(source.PmsProviderId,0) ELSE target.PmsProviderId END,
		target.PatientNumber		= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PatientNumber)))<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.PatientNumber)))		
			THEN TRIM(source.PatientNumber)		ELSE TRIM(target.PatientNumber) END,
		
		target.FirstName			= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.FirstName)))	<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.FirstName)))			
			THEN Trim(source.FirstName)			ELSE Trim(target.FirstName) END,
		target.LastName				= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.LastName)))	<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.LastName)))			
			THEN Trim(source.LastName)			ELSE Trim(target.LastName) END,
		
		target.PrimaryPhone			= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PrimaryPhone)))		<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.PrimaryPhone)))		
			THEN Trim(source.PrimaryPhone)		ELSE Trim(target.PrimaryPhone) END,
		target.PrimaryPhoneType		= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PrimaryPhoneType)))	<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.PrimaryPhoneType)))		
			THEN Trim(source.PrimaryPhoneType)	ELSE Trim(target.PrimaryPhoneType) END,
		target.SecondaryPhone		= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.SecondaryPhone)))	<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.SecondaryPhone)))		
			THEN Trim(source.SecondaryPhone)	ELSE Trim(target.SecondaryPhone) END,
		target.SecondaryPhoneType	= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.SecondaryPhoneType)))<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.SecondaryPhoneType)))		
			THEN Trim(source.SecondaryPhoneType)ELSE Trim(target.SecondaryPhoneType) END,
		target.CellPhone			= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.CellPhone)))			<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.CellPhone)))		
			THEN Trim(source.CellPhone)			ELSE Trim(target.CellPhone) END,
		target.Email				= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Email)))				<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.Email)))		
			THEN Trim(source.Email)				ELSE Trim(target.Email) END,
		
		target.Address1				= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Address1)))			<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.Address1)))			
			THEN Trim(source.Address1)			ELSE Trim(target.Address1) END,
		target.Address2				= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Address2)))			<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.Address2)))			
			THEN Trim(source.Address2)			ELSE Trim(target.Address2) END,
		target.City					= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.City)))				<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.City)))				
			THEN Trim(source.City)				ELSE Trim(target.City) END,
		target.State				= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.State)))				<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.State)))				
			THEN Trim(source.State)				ELSE Trim(target.State) END,
		target.Country				= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Country)))			<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.Country)))				
			THEN Trim(source.Country)			ELSE Trim(target.Country) END,
		target.Zip					= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Zip)))				<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.Zip)))					
			THEN Trim(source.Zip)				ELSE Trim(target.Zip) END,
		
		target.DOB					= CASE WHEN CHECKSUM(source.DOB)											<> CHECKSUM(target.DOB)					
			THEN source.DOB						ELSE target.DOB END,
		target.SSN					= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.SSN)))				<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.SSN)))					
			THEN Trim(source.SSN)				ELSE Trim(target.SSN) END,
		target.Gender				= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Gender)))			<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.Gender)))				
			THEN Trim(source.Gender)			ELSE Trim(target.Gender) END,
		target.PreferredLanguage	= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PreferredLanguage)))	<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.PreferredLanguage)))	
			THEN Trim(source.PreferredLanguage)	ELSE Trim(target.PreferredLanguage) END,
		target.Title				= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Title)))				<> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.Title)))				
			THEN Trim(source.Title)				ELSE Trim(target.Title) END,
		
		target.CommunicationChoice1	= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.CommunicationChoice1))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.CommunicationChoice1)))	
			THEN Trim(source.CommunicationChoice1)	ELSE Trim(target.CommunicationChoice1) END,
		target.CommunicationChoice2	= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.CommunicationChoice2))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.CommunicationChoice2)))	
			THEN Trim(source.CommunicationChoice2)	ELSE Trim(target.CommunicationChoice2) END,
		target.CommunicationChoice3	= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.CommunicationChoice3))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.CommunicationChoice3)))	
			THEN Trim(source.CommunicationChoice3)	ELSE Trim(target.CommunicationChoice3) END,
		
		target.SurveyEmailPreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.SurveyEmailPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.SurveyEmailPreference)))		
			THEN source.SurveyEmailPreference	ELSE target.SurveyEmailPreference END,
		target.SurveySMSPreference		= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.SurveySMSPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.SurveySMSPreference)))			
			THEN source.SurveySMSPreference		ELSE target.SurveySMSPreference END,
		target.SurveyVoicePreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.SurveyVoicePreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.SurveyVoicePreference)))		
			THEN source.SurveyVoicePreference	ELSE target.SurveyVoicePreference END,
		target.SurveyPostalPreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.SurveyPostalPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.SurveyPostalPreference)))		
			THEN source.SurveyPostalPreference	ELSE target.SurveyPostalPreference END,

		target.ReminderEmailPreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.ReminderEmailPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.ReminderEmailPreference)))		
			THEN source.ReminderEmailPreference	ELSE target.ReminderEmailPreference END,
		target.ReminderSMSPreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.ReminderSMSPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.ReminderSMSPreference)))		
			THEN source.ReminderSMSPreference	ELSE target.ReminderSMSPreference END,
		target.ReminderVoicePreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.ReminderVoicePreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.ReminderVoicePreference)))		
			THEN source.ReminderVoicePreference	ELSE target.ReminderVoicePreference END,
		target.ReminderPostalPreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.ReminderPostalPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.ReminderPostalPreference)))	
			THEN source.ReminderPostalPreference	ELSE target.ReminderPostalPreference END,

		target.RecallEmailPreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.RecallEmailPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.RecallEmailPreference)))		
			THEN source.RecallEmailPreference	ELSE target.RecallEmailPreference END,
		target.RecallSMSPreference		= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.RecallSMSPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.RecallSMSPreference)))			
			THEN source.RecallSMSPreference		ELSE target.RecallSMSPreference END,
		target.RecallVoicePreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.RecallVoicePreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.RecallVoicePreference)))		
			THEN source.RecallVoicePreference	ELSE target.RecallVoicePreference END,
		target.RecallPostalPreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.RecallPostalPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.RecallPostalPreference)))		
			THEN source.RecallPostalPreference	ELSE target.RecallPostalPreference END,

		target.PickupEmailPreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.PickupEmailPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.PickupEmailPreference)))		
			THEN source.PickupEmailPreference	ELSE target.PickupEmailPreference END,
		target.PickupSMSPreference		= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.PickupSMSPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.PickupSMSPreference)))			
			THEN source.PickupSMSPreference		ELSE target.PickupSMSPreference END,
		target.PickupVoicePreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.PickupVoicePreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.PickupVoicePreference)))		
			THEN source.PickupVoicePreference	ELSE target.PickupVoicePreference END,
		target.PickupPostalPreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.PickupPostalPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.PickupPostalPreference)))		
			THEN source.PickupPostalPreference	ELSE target.PickupPostalPreference END,

		target.OtherEmailPreference		= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.OtherEmailPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.OtherEmailPreference)))		
			THEN source.OtherEmailPreference	ELSE target.OtherEmailPreference END,
		target.OtherSMSPreference		= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.OtherSMSPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.OtherSMSPreference)))			
			THEN source.OtherSMSPreference		ELSE target.OtherSMSPreference END,
		target.OtherVoicePreference		= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.OtherVoicePreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.OtherVoicePreference)))		
			THEN source.OtherVoicePreference	ELSE target.OtherVoicePreference END,
		target.OtherPostalPreference	= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.OtherPostalPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.OtherPostalPreference)))		
			THEN source.OtherPostalPreference	ELSE target.OtherPostalPreference END,		
		
		target.Status					= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.Status))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.Status)))		
			THEN source.Status					ELSE target.Status END,

		target.ICD1						= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.ICD1))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.ICD1)))		
			THEN source.ICD1					ELSE Trim(target.ICD1) END,
		target.ICD2						= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.ICD2))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.ICD2)))		
			THEN source.ICD2					ELSE Trim(target.ICD2) END,		
		target.ICD3						= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.ICD3))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.ICD3)))		
			THEN source.ICD3					ELSE Trim(target.ICD3) END,

		target.ICD1Date					= CASE WHEN CHECKSUM(source.ICD1Date)	<> CHECKSUM(target.ICD1Date)					
			THEN source.ICD1Date				ELSE target.ICD1Date END,
		target.ICD2Date					= CASE WHEN CHECKSUM(source.ICD2Date)	<> CHECKSUM(target.ICD2Date)					
			THEN source.ICD2Date				ELSE target.ICD2Date END,
		target.ICD3Date					= CASE WHEN CHECKSUM(source.ICD3Date)	<> CHECKSUM(target.ICD3Date)					
			THEN source.ICD3Date				ELSE target.ICD3Date END,

		target.InsurerIDPrimary				= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.InsurerIDPrimary))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.InsurerIDPrimary)))			
			THEN source.InsurerIDPrimary		ELSE target.InsurerIDPrimary END,
		target.InsurerIDSecondary			= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.InsurerIDSecondary))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.InsurerIDSecondary)))			
			THEN source.InsurerIDSecondary		ELSE target.InsurerIDSecondary END,
		target.InsurerIDTertiary			= CASE WHEN CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.InsurerIDTertiary))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), target.InsurerIDTertiary)))			
			THEN source.InsurerIDTertiary		ELSE target.InsurerIDTertiary END,

		target.SpendingBehavior			= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.SpendingBehavior)))	  <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.SpendingBehavior)))
			THEN Trim(source.SpendingBehavior)		ELSE Trim(target.SpendingBehavior) END,
		target.ContactLensWearer		= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.ContactLensWearer)))	  <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.ContactLensWearer)))
			THEN 
				(CASE WHEN target.ContactLensWearer IS NOT NULL 
				THEN 
					Trim(target.ContactLensWearer) 
				ELSE Trim(source.ContactLensWearer) 
				END)
			ELSE Trim(target.ContactLensWearer) 
			END,
		target.PrimaryInsuranceEIN		= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PrimaryInsuranceEIN))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.PrimaryInsuranceEIN)))
			THEN Trim(source.PrimaryInsuranceEIN)	ELSE Trim(target.PrimaryInsuranceEIN) END,

		target.PrimaryInsuranceGroupId = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PrimaryInsuranceGroupId))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.PrimaryInsuranceGroupId)))
			THEN TRIM(source.PrimaryInsuranceGroupId) ELSE TRIM(target.PrimaryInsuranceGroupId) END,
		target.SecondaryInsuranceGroupId = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.SecondaryInsuranceGroupId))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.SecondaryInsuranceGroupId)))
			THEN TRIM(source.SecondaryInsuranceGroupId) ELSE TRIM(target.SecondaryInsuranceGroupId) END,
		target.TertiaryInsuranceGroupId = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.TertiaryInsuranceGroupId))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.TertiaryInsuranceGroupId)))
			THEN TRIM(source.TertiaryInsuranceGroupId) ELSE TRIM(target.TertiaryInsuranceGroupId) END,

		target.PrimaryInsurancePolicyNo = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PrimaryInsurancePolicyNo))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.PrimaryInsurancePolicyNo)))
			THEN TRIM(source.PrimaryInsurancePolicyNo) ELSE TRIM(target.PrimaryInsurancePolicyNo) END,
		target.SecondaryInsurancePolicyNo = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.SecondaryInsurancePolicyNo))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.SecondaryInsurancePolicyNo)))
			THEN TRIM(source.SecondaryInsurancePolicyNo) ELSE TRIM(target.SecondaryInsurancePolicyNo) END,
		target.TertiaryInsurancePolicyNo = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.TertiaryInsurancePolicyNo))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.TertiaryInsurancePolicyNo)))
			THEN TRIM(source.TertiaryInsurancePolicyNo) ELSE TRIM(target.TertiaryInsurancePolicyNo) END,
		target.PrimaryInsurancePlanName = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PrimaryInsurancePlanName))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.PrimaryInsurancePlanName)))
			THEN TRIM(source.PrimaryInsurancePlanName) ELSE TRIM(target.PrimaryInsurancePlanName) END,
		target.SecondaryInsurancePlanName = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.SecondaryInsurancePlanName))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.SecondaryInsurancePlanName)))
			THEN TRIM(source.SecondaryInsurancePlanName) ELSE TRIM(target.SecondaryInsurancePlanName) END,
		target.TertiaryInsurancePlanName = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.TertiaryInsurancePlanName))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.TertiaryInsurancePlanName)))
			THEN TRIM(source.TertiaryInsurancePlanName) ELSE TRIM(target.TertiaryInsurancePlanName) END,

		target.PmsSpecificName1 = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PmsSpecificName1))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.PmsSpecificName1)))
			THEN TRIM(source.PmsSpecificName1) ELSE TRIM(target.PmsSpecificName1) END,
		target.PmsSpecificValue1 = CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PmsSpecificValue1))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(target.PmsSpecificValue1)))
			THEN TRIM(source.PmsSpecificValue1) ELSE TRIM(target.PmsSpecificValue1) END,			
		target.LastVisitOrExamDate			= CASE WHEN CHECKSUM(source.LastVisitOrExamDate)		<> CHECKSUM(target.LastVisitOrExamDate)			
			THEN source.LastVisitOrExamDate	
				(CASE WHEN source.LastVisitOrExamDate > target.LastVisitOrExamDate 
				THEN source.LastVisitOrExamDate 
				ELSE target.LastVisitOrExamDate 
				END)
			ELSE target.LastVisitOrExamDate 
			END,
        
		target.Deleted						= CASE WHEN CHECKSUM(source.Deleted)					<> CHECKSUM(target.Deleted)						
			THEN source.Deleted ELSE target.Deleted END,
		
		target.PmsCreatedDate				= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), source.PmsCreatedDate)) <> CHECKSUM(CONVERT(VARCHAR(MAX), target.PmsCreatedDate)) 
			THEN source.PmsCreatedDate ELSE target.PmsCreatedDate END,
		
		target.PmsLastModifiedDate			= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), source.PmsLastModifiedDate)) <> CHECKSUM(CONVERT(VARCHAR(MAX), target.PmsLastModifiedDate)) 
			THEN source.PmsLastModifiedDate ELSE target.PmsLastModifiedDate END,

		target.UTCCreatedDate				= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), source.UTCCreatedDate)) <> CHECKSUM(CONVERT(VARCHAR(MAX), target.UTCCreatedDate)) 
			THEN source.UTCCreatedDate ELSE target.UTCCreatedDate END,

		target.UTCLastModifiedDate			= CASE WHEN CHECKSUM(CONVERT(VARCHAR(MAX), source.UTCLastModifiedDate)) <> CHECKSUM(CONVERT(VARCHAR(MAX), target.UTCLastModifiedDate)) 
			THEN source.UTCLastModifiedDate ELSE target.UTCLastModifiedDate END
 -- output to delta table the values of the changed columns, or null if the columns didn't change
 -- using the CHECKSUM function is more efficient than comparing the actual string values
 -- the deleted identifier provides the old values 
 OUTPUT
    source.PmsInstanceId,
	source.PmsPatientId,
    CASE 
        WHEN $action = 'INSERT' THEN source.UTCCreatedDate
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.UTCCreatedDate))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.UTCCreatedDate))) 
            THEN source.UTCCreatedDate 
        ELSE NULL 
    END AS UTcCreatedDate,
    CASE 
        WHEN $action = 'INSERT' THEN 1 
        WHEN $action = 'UPDATE' THEN 2
    END AS ActionPerformed,

    CASE 
        WHEN $action = 'INSERT' THEN source.PmsLocationId
        WHEN $action = 'UPDATE' AND CHECKSUM(source.PmsLocationId) <> CHECKSUM(deleted.PmsLocationId) 
			THEN source.PmsLocationId 
        ELSE NULL 
    END AS PmsLocationId,
    CASE 
        WHEN $action = 'INSERT' THEN source.PmsProviderId
        WHEN $action = 'UPDATE' AND CHECKSUM(source.PmsProviderId) <> CHECKSUM(deleted.PmsProviderId) 
			THEN source.PmsProviderId 
        ELSE NULL 
    END AS PmsProviderId,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.PatientNumber)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PatientNumber))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.PatientNumber))) 
			THEN Trim(source.PatientNumber) 
        ELSE NULL 
    END AS PatientNumber,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.FirstName)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.FirstName))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.FirstName))) 
			THEN Trim(source.FirstName)
        ELSE NULL 
    END AS FirstName,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.LastName)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.LastName))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.LastName))) 
			THEN Trim(source.LastName)
        ELSE NULL 
    END AS LastName,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.PrimaryPhone)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PrimaryPhone))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.PrimaryPhone))) 
			THEN Trim(source.PrimaryPhone) 
        ELSE NULL 
    END AS PrimaryPhone,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.PrimaryPhoneType)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PrimaryPhoneType))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.PrimaryPhoneType))) 
			THEN Trim(source.PrimaryPhoneType) 
        ELSE NULL 
    END AS PrimaryPhoneType,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.SecondaryPhone)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.SecondaryPhone))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.SecondaryPhone))) 
			THEN Trim(source.SecondaryPhone) 
        ELSE NULL 
    END AS SecondaryPhone,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.SecondaryPhoneType)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.SecondaryPhoneType))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.SecondaryPhoneType))) 
			THEN Trim(source.SecondaryPhoneType) 
        ELSE NULL 
    END AS SecondaryPhoneType,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.CellPhone)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.CellPhone))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.CellPhone))) 
			THEN Trim(source.CellPhone )
        ELSE NULL 
    END AS CellPhone,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.Email)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Email))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.Email))) 
			THEN Trim(source.Email)
        ELSE NULL 
    END AS Email,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.Address1)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Address1))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.Address1))) 
			THEN Trim(source.Address1 )
        ELSE NULL 
    END AS Address1,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.Address2)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Address2))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.Address2))) 
			THEN Trim(source.Address2) 
        ELSE NULL 
    END AS Address2,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.City)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.City))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.City))) 
			THEN Trim(source.City)
        ELSE NULL 
    END AS City,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.State)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.State))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.State))) 
			THEN Trim(source.State)
        ELSE NULL 
    END AS State,    
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.Country)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Country))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.Country))) 
			THEN Trim(source.Country)
        ELSE NULL 
    END AS Country,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.Zip)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Zip))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.Zip))) 
			THEN Trim(source.Zip)
        ELSE NULL 
    END AS Zip,
    CASE 
        WHEN $action = 'INSERT' THEN source.DOB
        WHEN $action = 'UPDATE' AND CHECKSUM(source.DOB) <> CHECKSUM(deleted.DOB) 
			THEN source.DOB 
        ELSE NULL 
    END AS DOB,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.SSN)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.SSN))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.SSN))) 
			THEN Trim(source.SSN) 
        ELSE NULL 
    END AS SSN,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.Gender)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Gender))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.Gender))) 
			THEN Trim(source.Gender)
        ELSE NULL 
    END AS Gender,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.PreferredLanguage)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.PreferredLanguage))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.PreferredLanguage))) 
			THEN Trim(source.PreferredLanguage) 
        ELSE NULL 
    END AS PreferredLanguage,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.Title)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.Title))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.Title))) 
			THEN Trim(source.Title )
        ELSE NULL 
    END AS Title,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.CommunicationChoice1)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.CommunicationChoice1))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.CommunicationChoice1))) 
			THEN Trim(source.CommunicationChoice1)
        ELSE NULL 
    END AS CommunicationChoice1,
    CASE 
        WHEN $action = 'INSERT' THEN source.CommunicationChoice2
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.CommunicationChoice2))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.CommunicationChoice2))) 
			THEN Trim(source.CommunicationChoice2)
        ELSE NULL 
    END AS CommunicationChoice2,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.CommunicationChoice3)
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(source.CommunicationChoice3))) <> CHECKSUM(CONVERT(VARCHAR(MAX), TRIM(deleted.CommunicationChoice3))) 
			THEN Trim(source.CommunicationChoice3)
        ELSE NULL 
    END AS CommunicationChoice3,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.SurveyEmailPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.SurveyEmailPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.SurveyEmailPreference)))
			THEN source.SurveyEmailPreference
		ELSE NULL 
	END AS SurveyEmailPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.SurveySMSPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.SurveySMSPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.SurveySMSPreference)))
			THEN source.SurveySMSPreference
		ELSE NULL 
	END AS SurveySMSPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.SurveyVoicePreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.SurveyVoicePreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.SurveyVoicePreference)))
			THEN source.SurveyVoicePreference
		ELSE NULL 
	END AS SurveyVoicePreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.SurveyPostalPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.SurveyPostalPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.SurveyPostalPreference)))
			THEN source.SurveyPostalPreference
		ELSE NULL 
	END AS SurveyPostalPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.ReminderEmailPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.ReminderEmailPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.ReminderEmailPreference)))
			THEN source.ReminderEmailPreference
		ELSE NULL 
	END AS ReminderEmailPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.ReminderSMSPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.ReminderSMSPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.ReminderSMSPreference)))
			THEN source.ReminderSMSPreference
		ELSE NULL 
	END AS ReminderSMSPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.ReminderVoicePreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.ReminderVoicePreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.ReminderVoicePreference)))
			THEN source.ReminderVoicePreference
		ELSE NULL 
	END AS ReminderVoicePreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.ReminderPostalPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.ReminderPostalPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.ReminderPostalPreference)))
			THEN source.ReminderPostalPreference
		ELSE NULL 
	END AS ReminderPostalPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.RecallEmailPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.RecallEmailPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.RecallEmailPreference)))
			THEN source.RecallEmailPreference
		ELSE NULL 
	END AS RecallEmailPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.RecallSMSPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.RecallSMSPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.RecallSMSPreference)))
			THEN source.RecallSMSPreference
		ELSE NULL 
	END AS RecallSMSPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.RecallVoicePreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.RecallVoicePreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.RecallVoicePreference)))
			THEN source.RecallVoicePreference
		ELSE NULL 
	END AS RecallVoicePreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.RecallPostalPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.RecallPostalPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.RecallPostalPreference)))
			THEN source.RecallPostalPreference
		ELSE NULL 
	END AS RecallPostalPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.PickupEmailPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.PickupEmailPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.PickupEmailPreference)))
			THEN source.PickupEmailPreference
		ELSE NULL 
	END AS PickupEmailPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.PickupSMSPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.PickupSMSPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.PickupSMSPreference)))
			THEN source.PickupSMSPreference
		ELSE NULL 
	END AS PickupSMSPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.PickupVoicePreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.PickupVoicePreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.PickupVoicePreference)))
			THEN source.PickupVoicePreference
		ELSE NULL 
	END AS PickupVoicePreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.PickupPostalPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.PickupPostalPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.PickupPostalPreference)))
			THEN source.PickupPostalPreference
		ELSE NULL 
	END AS PickupPostalPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.OtherEmailPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.OtherEmailPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.OtherEmailPreference)))
			THEN source.OtherEmailPreference
		ELSE NULL 
	END AS OtherEmailPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.OtherSMSPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.OtherSMSPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.OtherSMSPreference)))
			THEN source.OtherSMSPreference
		ELSE NULL 
	END AS OtherSMSPreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.OtherVoicePreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.OtherVoicePreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.OtherVoicePreference)))
			THEN source.OtherVoicePreference
		ELSE NULL 
	END AS OtherVoicePreference,
	CASE 
		WHEN $action = 'INSERT' THEN ISNULL(source.OtherPostalPreference, 0)
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.OtherPostalPreference))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.OtherPostalPreference)))
			THEN source.OtherPostalPreference
		ELSE NULL 
	END AS OtherPostalPreference,

	CASE 
        WHEN $action = 'INSERT' THEN source.Status
		WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.Status))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.Status)))
			THEN source.Status 
        ELSE NULL 
    END AS Status,
    CASE 
        WHEN $action = 'INSERT' THEN TRIM(source.ICD1)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ICD1))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.ICD1))) 
			THEN TRIM(source.ICD1) 
        ELSE NULL 
    END AS ICD1,
    
    CASE 
        WHEN $action = 'INSERT' THEN TRIM(source.ICD2)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ICD2))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.ICD2))) 
			THEN TRIM(source.ICD2) 
        ELSE NULL 
    END AS ICD2,
    
    CASE 
        WHEN $action = 'INSERT' THEN TRIM(source.ICD3)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ICD3))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.ICD3))) 
			THEN TRIM(source.ICD3) 
        ELSE NULL 
    END AS ICD3,
    CASE 
        WHEN $action = 'INSERT' THEN source.ICD1Date
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), source.ICD1Date)) <> CHECKSUM(CONVERT(VARCHAR(MAX), deleted.ICD1Date)) 
			THEN source.ICD1Date 
        ELSE NULL 
    END AS ICD1Date,
    
    CASE 
        WHEN $action = 'INSERT' THEN source.ICD2Date
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), source.ICD2Date)) <> CHECKSUM(CONVERT(VARCHAR(MAX), deleted.ICD2Date)) 
			THEN source.ICD2Date 
        ELSE NULL 
    END AS ICD2Date,
    
    CASE 
        WHEN $action = 'INSERT' THEN source.ICD3Date
        WHEN $action = 'UPDATE' AND CHECKSUM(CONVERT(VARCHAR(MAX), source.ICD3Date)) <> CHECKSUM(CONVERT(VARCHAR(MAX), deleted.ICD3Date)) 
			THEN source.ICD3Date 
        ELSE NULL 
    END AS ICD3Date,
    CASE 
        WHEN $action = 'INSERT' THEN ISNULL(source.InsurerIDPrimary,0)
        WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.InsurerIDPrimary))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.InsurerIDPrimary)))
			THEN source.InsurerIDPrimary
        ELSE NULL 
    END AS InsurerIDPrimary,
    CASE 
        WHEN $action = 'INSERT' THEN ISNULL(source.InsurerIDSecondary,0)
        WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.InsurerIDSecondary))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.InsurerIDSecondary)))
			THEN source.InsurerIDSecondary
        ELSE NULL 
    END AS InsurerIDSecondary,
    CASE 
        WHEN $action = 'INSERT' THEN ISNULL(source.InsurerIDTertiary,0)
        WHEN $action = 'UPDATE' AND CHECKSUM(Trim(CONVERT(VARCHAR(MAX), source.InsurerIDTertiary))) <> CHECKSUM(Trim(CONVERT(VARCHAR(MAX), deleted.InsurerIDTertiary)))
			THEN source.InsurerIDTertiary
        ELSE NULL 
    END AS InsurerIDTertiary,

    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.SpendingBehavior)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.SpendingBehavior))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.SpendingBehavior))) 
			THEN Trim(source.SpendingBehavior) 
        ELSE NULL 
    END AS SpendingBehavior,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.ContactLensWearer)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.ContactLensWearer))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.ContactLensWearer))) 
			THEN Trim(source.ContactLensWearer)
        ELSE NULL 
    END AS ContactLensWearer,
    CASE 
        WHEN $action = 'INSERT' THEN Trim(source.PrimaryInsuranceEIN)
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.PrimaryInsuranceEIN))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.PrimaryInsuranceEIN))) 
			THEN Trim(source.PrimaryInsuranceEIN) 
        ELSE NULL 
    END AS PrimaryInsuranceEIN,
	CASE 
		WHEN $action = 'INSERT' THEN Trim(source.PrimaryInsuranceGroupId)
		WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.PrimaryInsuranceGroupId))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.PrimaryInsuranceGroupId))) 
			THEN Trim(source.PrimaryInsuranceGroupId) 
		ELSE NULL 
	END AS PrimaryInsuranceGroupId,
	CASE 
		WHEN $action = 'INSERT' THEN Trim(source.SecondaryInsuranceGroupId)
		WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.SecondaryInsuranceGroupId))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.SecondaryInsuranceGroupId))) 
			THEN Trim(source.SecondaryInsuranceGroupId) 
		ELSE NULL 
	END AS SecondaryInsuranceGroupId,
	CASE 
		WHEN $action = 'INSERT' THEN Trim(source.TertiaryInsuranceGroupId)
		WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.TertiaryInsuranceGroupId))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.TertiaryInsuranceGroupId))) 
			THEN Trim(source.TertiaryInsuranceGroupId) 
		ELSE NULL 
	END AS TertiaryInsuranceGroupId,
	CASE 
		WHEN $action = 'INSERT' THEN Trim(source.PrimaryInsurancePolicyNo)
		WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.PrimaryInsurancePolicyNo))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.PrimaryInsurancePolicyNo))) 
			THEN Trim(source.PrimaryInsurancePolicyNo) 
		ELSE NULL 
	END AS PrimaryInsurancePolicyNo,
	CASE 
		WHEN $action = 'INSERT' THEN Trim(source.SecondaryInsurancePolicyNo)
		WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.SecondaryInsurancePolicyNo))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.SecondaryInsurancePolicyNo))) 
			THEN Trim(source.SecondaryInsurancePolicyNo) 
		ELSE NULL 
	END AS SecondaryInsurancePolicyNo,
	CASE 
		WHEN $action = 'INSERT' THEN Trim(source.TertiaryInsurancePolicyNo)
		WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.TertiaryInsurancePolicyNo))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.TertiaryInsurancePolicyNo))) 
			THEN Trim(source.TertiaryInsurancePolicyNo) 
		ELSE NULL 
	END AS TertiaryInsurancePolicyNo,
	CASE 
		WHEN $action = 'INSERT' THEN Trim(source.PrimaryInsurancePlanName)
		WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.PrimaryInsurancePlanName))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.PrimaryInsurancePlanName))) 
			THEN Trim(source.PrimaryInsurancePlanName) 
		ELSE NULL 
	END AS PrimaryInsurancePlanName,
	CASE 
		WHEN $action = 'INSERT' THEN Trim(source.SecondaryInsurancePlanName)
		WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.SecondaryInsurancePlanName))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.SecondaryInsurancePlanName))) 
			THEN Trim(source.SecondaryInsurancePlanName) 
		ELSE NULL 
	END AS SecondaryInsurancePlanName,
	CASE 
		WHEN $action = 'INSERT' THEN Trim(source.TertiaryInsurancePlanName)
		WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.TertiaryInsurancePlanName))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.TertiaryInsurancePlanName))) 
			THEN Trim(source.TertiaryInsurancePlanName) 
		ELSE NULL 
	END AS TertiaryInsurancePlanName,
	CASE 
		WHEN $action = 'INSERT' THEN Trim(source.PmsSpecificName1)
		WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.PmsSpecificName1))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.PmsSpecificName1))) 
			THEN Trim(source.PmsSpecificName1) 
		ELSE NULL 
	END AS PmsSpecificName1,
	CASE 
		WHEN $action = 'INSERT' THEN Trim(source.PmsSpecificValue1)
		WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.PmsSpecificValue1))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.PmsSpecificValue1))) 
			THEN Trim(source.PmsSpecificValue1) 
		ELSE NULL 
	END AS PmsSpecificValue1,
	CASE 
        WHEN $action = 'INSERT' THEN source.LastVisitOrExamDate
        WHEN $action = 'UPDATE' AND CHECKSUM(source.LastVisitOrExamDate) <> CHECKSUM(deleted.LastVisitOrExamDate) 
			THEN source.LastVisitOrExamDate 
        ELSE NULL 
    END AS LastVisitOrExamDate,
    CASE 
        WHEN $action = 'INSERT' THEN source.Deleted
        WHEN $action = 'UPDATE' AND CHECKSUM(source.Deleted) <> CHECKSUM(deleted.Deleted) 
			THEN source.Deleted 
        ELSE NULL 
    END AS Deleted,
    CASE 
        WHEN $action = 'INSERT' THEN source.PmsCreatedDate
        WHEN $action = 'UPDATE' AND CHECKSUM(source.PmsCreatedDate) <> CHECKSUM(deleted.PmsCreatedDate) 
			THEN source.PmsCreatedDate 
        ELSE NULL 
    END AS PmsCreatedDate,
    CASE 
        WHEN $action = 'INSERT' THEN source.PmsLastModifiedDate
        WHEN $action = 'UPDATE' AND CHECKSUM(source.PmsLastModifiedDate) <> CHECKSUM(deleted.PmsLastModifiedDate) 
			THEN source.PmsLastModifiedDate 
        ELSE NULL 
    END AS PmsLastModifiedDate,
    CASE 
        WHEN $action = 'INSERT' THEN source.UtcLastModifiedDate
        WHEN $action = 'UPDATE' AND CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), source.UtcLastModifiedDate))) <> CHECKSUM(TRIM(CONVERT(VARCHAR(MAX), deleted.UtcLastModifiedDate))) 
			THEN source.UtcLastModifiedDate 
        ELSE NULL 
    END AS UtcLastModifiedDate   
	INTO [shadow].[PatientDelta]
	(
		PmsInstanceId,
		PmsPatientId,
		UtcCreatedDate,
		ActionPerformed,
		PmsLocationId,
		PmsProviderId,
		PatientNumber,
		FirstName,
		LastName,
		PrimaryPhone,
		PrimaryPhoneType,
		SecondaryPhone,
		SecondaryPhoneType,
		CellPhone,
		Email,
		Address1,
		Address2,
		City,
		State,
		Country,
		Zip,
		DOB,
		SSN,
		Gender,
		PreferredLanguage,
		Title,
		CommunicationChoice1,
		CommunicationChoice2,
		CommunicationChoice3,
		SurveyEmailPreference,
		SurveySMSPreference,
		SurveyVoicePreference,
		SurveyPostalPreference,
		ReminderEmailPreference,
		ReminderSMSPreference,
		ReminderVoicePreference,
		ReminderPostalPreference,
		RecallEmailPreference,
		RecallSMSPreference,
		RecallVoicePreference,
		RecallPostalPreference,
		PickupEmailPreference,
		PickupSMSPreference,
		PickupVoicePreference,
		PickupPostalPreference,
		OtherEmailPreference,
		OtherSMSPreference,
		OtherVoicePreference,
		OtherPostalPreference,
		Status,
		ICD1,
		ICD2,
		ICD3,
		ICD1Date,
		ICD2Date,
		ICD3Date,
		InsurerIDPrimary,
		InsurerIDSecondary,
		InsurerIDTertiary,
		SpendingBehavior,
		ContactLensWearer,
		PrimaryInsuranceEIN,
		PrimaryInsuranceGroupId,
		SecondaryInsuranceGroupId,
		TertiaryInsuranceGroupId,
		PrimaryInsurancePolicyNo,
		SecondaryInsurancePolicyNo,
		TertiaryInsurancePolicyNo,
		PrimaryInsurancePlanName,
		SecondaryInsurancePlanName,
		TertiaryInsurancePlanName,
		PmsSpecificName1,
		PmsSpecificValue1,
		LastVisitOrExamDate,
		Deleted,
		PmsCreatedDate,
		PmsLastModifiedDate,
		UTCLastModifiedDate
	);

	drop table #temp
   
	SET NOCOUNT OFF;
END;
GO

CREATE PROCEDURE [shadow].[sp_ProductPickupMarkDeleted]
	@paramUtcOffset int,
	@inputData [shadow].udtt_PmsId readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	MERGE INTO [shadow].ProductPickup as target
	USING @inputData as source
	on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsOrderId) = TRIM(source.PmsId)
	WHEN MATCHED THEN
	UPDATE 
	SET 
		Deleted  = 1,
		UtcLastModifiedDate = @utcDate
	OUTPUT   -- delta table
		source.PmsInstanceId,
		TRIM(source.PmsId),
		@utcDate,
		3 AS ActionPerformed
	INTO [shadow].[ProductPickupDelta](PmsInstanceId, PmsOrderId, UtcCreatedDate, ActionPerformed); 
    
END
GO

CREATE PROCEDURE [shadow].[sp_ProductPickupUpsert]
	@paramUtcOffset int,
	@inputData shadow.udtt_ProductPickup readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	select 
		input.PmsInstanceId,
		input.PmsOrderId,
		pat.PmsPatientId,
		CASE WHEN (TRIM(input.PmsLocationId) is null or TRIM(input.PmsLocationId)='') THEN TRIM(pat.PmsLocationId) ELSE TRIM(loc.PmsLocationId)  END AS PmsLocationId,
		CASE WHEN (TRIM(input.PmsProviderId) is null or TRIM(input.PmsProviderId)='') THEN TRIM(pat.PmsProviderId) ELSE TRIM(prov.PmsProviderId) END AS PmsProviderId,
		input.OrderDate,
		input.ReceivedDate,
		input.NotifiedDate,
		input.PickedUpDate,
		input.ProductType,
		input.ProductBrand1,
		input.ProductBrand2,
		input.ProductBrand3,
		input.SpendAmt,
		input.OrderStatus,
		input.PricedDate,
		input.StatusChangedDate,
		input.Deleted,
		input.PmsCreatedDate,
		input.PmsLastModifiedDate,
		input.UTCCreatedDate,
		input.UTCLastModifiedDate
	into #temp
	from @inputData input
	left outer join [shadow].Location loc  on loc.PmsLocationId  = input.PmsLocationId AND loc.PmsInstanceId  = input.PmsInstanceId
	left outer join [shadow].Provider prov on prov.PmsProviderId = input.PmsProviderId AND prov.PmsInstanceId = input.PmsInstanceId
	left outer join [shadow].Patient pat   on pat.PmsPatientId   = input.PmsPatientId  AND pat.PmsInstanceId  = input.PmsInstanceId

	Merge into [shadow].Patient as target
	using (Select Distinct PmsInstanceId, PmsPatientId From #temp Where TRIM(ProductType)='Contact Lenses' Group By PmsInstanceId,PmsPatientId) as source
	on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsPatientId) = TRIM(source.PmsPatientId) 
	WHEN MATCHED THEN
	UPDATE
		set target.ContactLensWearer = 'Y',
			target.UtcLastModifiedDate = @utcDate; --  end merge

	MERGE INTO [shadow].[ProductPickup] as target
	USING #temp as source
	on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsOrderId) = TRIM(source.PmsOrderId)
	WHEN NOT MATCHED BY TARGET 
	THEN
		INSERT (
			PmsInstanceId, 
			PmsOrderId,
			PmsPatientId,
			PmsLocationId,
			PmsProviderId,
			OrderDate,
			ReceivedDate,
			NotifiedDate,
			PickedUpDate,
			ProductType,
			ProductBrand1,
			ProductBrand2,
			ProductBrand3,
			SpendAmt,
			OrderStatus,
			PricedDate,
			StatusChangedDate,
			Deleted,
			PmsCreatedDate,
			PmsLastModifiedDate,
			UTCCreatedDate,
			UTCLastModifiedDate)
		VALUES(
			source.PmsInstanceId, 
			TRIM(source.PmsOrderId),
			ISNULL(source.PmsPatientId,0),
			ISNULL(source.PmsLocationId,0),
			ISNULL(source.PmsProviderId,0),
			source.OrderDate,
			source.ReceivedDate,
			source.NotifiedDate,
			source.PickedUpDate,
			TRIM(source.ProductType),
			TRIM(source.ProductBrand1),
			TRIM(source.ProductBrand2),
			TRIM(source.ProductBrand3),
			source.SpendAmt,
			TRIM(source.OrderStatus),
			source.PricedDate,
			source.StatusChangedDate,
			source.Deleted,
			source.PmsCreatedDate,
			source.PmsLastModifiedDate,
			@utcDate, 
			@utcDate)
	WHEN MATCHED AND
	(
		CHECKSUM
		(
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.PmsOrderId),CONVERT(VARCHAR(MAX),'')))	+
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsPatientId,0))		+	
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsLocationId,0))		+
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsProviderId,0))		+
			ISNULL(CONVERT(VARCHAR(MAX),source.OrderDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.ReceivedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.NotifiedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PickedUpDate),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ProductType),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ProductBrand1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ProductBrand2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ProductBrand3),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.SpendAmt),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.OrderStatus),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PricedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.StatusChangedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.Deleted),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PMSCreatedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PMSLastModifiedDate),CONVERT(VARCHAR(MAX),''))
		)
		<>
		CHECKSUM
		(
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.PmsOrderId),CONVERT(VARCHAR(MAX),'')))	+
			CONVERT(VARCHAR(MAX), target.PmsPatientId)		+	
			CONVERT(VARCHAR(MAX), target.PmsLocationId)		+
			CONVERT(VARCHAR(MAX), target.PmsProviderId)		+
			ISNULL(CONVERT(VARCHAR(MAX),target.OrderDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.ReceivedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.NotifiedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PickedUpDate),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ProductType),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ProductBrand1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ProductBrand2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ProductBrand3),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.SpendAmt),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.OrderStatus),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PricedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.StatusChangedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.Deleted),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PMSCreatedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PMSLastModifiedDate),CONVERT(VARCHAR(MAX),''))
		)
	)
	THEN
		UPDATE SET 
				--target.PmsInstanceId		= TRIM(source.PmsInstanceId),  I don't think the PmsInstanceId should be updated - right?
				target.PmsOrderId			= TRIM(source.PmsOrderId),
				target.PmsPatientId			= ISNULL(TRIM(source.PmsPatientId),0),
				target.PmsLocationId		= ISNULL(TRIM(source.PmsLocationId),0),
				target.PmsProviderId		= ISNULL(TRIM(source.PmsProviderId),0),
				target.OrderDate			= source.OrderDate,
				target.ReceivedDate			= source.ReceivedDate,
				target.NotifiedDate			= source.NotifiedDate,
				target.PickedUpDate			= source.PickedUpDate,
				target.ProductType			= TRIM(source.ProductType),
				target.ProductBrand1		= TRIM(source.ProductBrand1),
				target.ProductBrand2		= TRIM(source.ProductBrand2),
				target.ProductBrand3		= TRIM(source.ProductBrand3),
				target.SpendAmt				= source.SpendAmt,
				target.OrderStatus			= TRIM(source.OrderStatus),
				target.PricedDate			= source.PricedDate,
				target.StatusChangedDate	= source.StatusChangedDate,
				target.Deleted				= source.Deleted,
				target.PmsCreatedDate		= source.PmsCreatedDate,
				target.PmsLastModifiedDate	= source.PmsLastModifiedDate,
				target.UtcLastModifiedDate = @utcDate
		OUTPUT   -- delta table
			source.PmsInstanceId,
			TRIM(source.PmsOrderId),
			@utcDate,
			CASE 
			WHEN $action = 'INSERT' THEN 1 
			WHEN $action = 'UPDATE' THEN 2
				END AS ActionPerformed
		INTO [shadow].[ProductPickupDelta](PmsInstanceId, PmsOrderId, UtcCreatedDate, ActionPerformed); 
 
	drop table #temp;

	SET NOCOUNT OFF;
END;
GO

CREATE  PROCEDURE [shadow].[sp_ProviderUpsert]
	@paramUtcOffset int,
	@inputData shadow.udtt_Provider readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	MERGE INTO [shadow].[Provider] as target
	USING @inputData as source
	on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsProviderId) = TRIM(source.PmsProviderId)
	WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (PmsInstanceId, PmsProviderId, ProviderFirstName,ProviderLastName,  UtcCreatedDate, UtcLastModifiedDate)
		VALUES (source.PmsInstanceId, TRIM(source.PmsProviderId), TRIM(source.ProviderFirstName), TRIM(source.ProviderLastName), @utcDate, @utcDate )
	WHEN MATCHED AND
	(
		CHECKSUM
		(
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ProviderFirstName),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ProviderLastName),CONVERT(VARCHAR(MAX),'')))
		)
		<>
		CHECKSUM
		(
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ProviderFirstName),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ProviderLastName),CONVERT(VARCHAR(MAX),'')))
		)
	)
	THEN
		UPDATE SET 
				target.ProviderFirstName = TRIM(source.ProviderFirstName), 
				target.ProviderLastName  = TRIM(source.ProviderLastName), 
				UtcLastModifiedDate = @utcDate
		OUTPUT   -- delta table
			source.PmsInstanceId,
			TRIM(source.PmsProviderId),
			@utcDate,
			CASE 
				WHEN $action = 'INSERT' THEN 1 
				WHEN $action = 'UPDATE' THEN 2
			 END AS ActionPerformed
		INTO [shadow].[ProviderDelta](PmsInstanceId, PmsProviderId, UtcCreatedDate, ActionPerformed); 
    
	SET NOCOUNT OFF;
END;
GO

CREATE PROCEDURE [shadow].[sp_RecallMarkDeleted]
	@paramUtcOffset int,
	@inputData [shadow].udtt_PmsId readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	MERGE INTO [shadow].Recall as target
	USING @inputData as source
	on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsRecallId) = TRIM(source.PmsId)
	WHEN MATCHED THEN
	UPDATE 
	SET 
		Deleted  = 1,
		UtcLastModifiedDate = @utcDate
	OUTPUT   -- delta table
		source.PmsInstanceId,
		TRIM(source.PmsId),
		@utcDate,
		3 AS ActionPerformed
	INTO [shadow].[RecallDelta](PmsInstanceId, PmsRecallId, UtcCreatedDate, ActionPerformed); 
END
GO

CREATE PROCEDURE [shadow].[sp_RecallReasonUpsert]
	@paramUtcOffset int,
	@nameOnly nvarchar(5),
	@inputData shadow.udtt_RecallReason readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	if @nameOnly = 'Y'
	BEGIN
		MERGE INTO [shadow].[RecallReason] as target
		USING @inputData as source
		on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.ReasonName) = TRIM(source.ReasonName)
		WHEN NOT MATCHED BY TARGET
		THEN
			INSERT (PmsInstanceId, PmsRecallReasonId, ReasonName, UtcCreatedDate, UtcLastModifiedDate)
			VALUES (source.PmsInstanceId, TRIM(source.PmsRecallReasonId), TRIM(source.ReasonName), @utcDate, @utcDate)
		WHEN MATCHED AND
		(
			CHECKSUM
			(
				TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.PmsRecallReasonId),CONVERT(VARCHAR(MAX),'')))
			)
			<>
			CHECKSUM
			(
				TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.PmsRecallReasonId),CONVERT(VARCHAR(MAX),'')))
			)
		)
		THEN
			UPDATE SET 
				target.PmsRecallReasonId = TRIM(source.PmsRecallReasonId),
				target.ReasonName  = TRIM(source.ReasonName), 
				UtcLastModifiedDate = @utcDate
			OUTPUT   -- delta table
				source.PmsInstanceId,
				TRIM(source.PmsRecallReasonId),
				@utcDate,
				CASE 
				WHEN $action = 'INSERT' THEN 1 
				WHEN $action = 'UPDATE' THEN 2
				 END AS ActionPerformed
			INTO [shadow].[RecallReasonDelta](PmsInstanceId, PmsRecallReasonId, UtcCreatedDate, ActionPerformed); 
	END
	ELSE
	BEGIN
		MERGE INTO [shadow].[RecallReason] as target
		USING @inputData as source
		on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsRecallReasonId) = TRIM(source.PmsRecallReasonId)
		WHEN NOT MATCHED BY TARGET
		THEN
			INSERT (PmsInstanceId, PmsRecallReasonId, ReasonName, UtcCreatedDate, UtcLastModifiedDate)
			VALUES (source.PmsInstanceId, TRIM(source.PmsRecallReasonId), TRIM(source.ReasonName), @utcDate, @utcDate)
		WHEN MATCHED AND
		(
			CHECKSUM
			(
				TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ReasonName),CONVERT(VARCHAR(MAX),'')))
			)
			<>
			CHECKSUM
			(
				TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ReasonName),CONVERT(VARCHAR(MAX),'')))
			)
		)
		THEN
			UPDATE SET 
				target.ReasonName  = TRIM(source.ReasonName), 
				UtcLastModifiedDate = @utcDate
			OUTPUT   -- delta table
				source.PmsInstanceId,
				TRIM(source.PmsRecallReasonId),
				@utcDate,
				CASE 
				WHEN $action = 'INSERT' THEN 1 
				WHEN $action = 'UPDATE' THEN 2
				 END AS ActionPerformed
			INTO [shadow].[RecallReasonDelta](PmsInstanceId, PmsRecallReasonId, UtcCreatedDate, ActionPerformed); 
	END
 
	SET NOCOUNT OFF;
END;
GO

CREATE PROCEDURE [shadow].[sp_RecallUpsert]
	@paramUtcOffset int,
	@inputData shadow.udtt_Recall readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	select 
		input.PmsInstanceId,
		input.PmsRecallId,
		pat.PmsPatientId,
		CASE WHEN (TRIM(input.PmsLocationId) is null or TRIM(input.PmsLocationId)='') THEN TRIM(pat.PmsLocationId) ELSE TRIM(loc.PmsLocationId)  END AS PmsLocationId ,
		CASE WHEN (TRIM(input.PmsProviderId) is null or TRIM(input.PmsProviderId)='') THEN TRIM(pat.PmsProviderId) ELSE TRIM(prov.PmsProviderId) END AS PmsProviderId,
		input.RecallDate,
		CASE WHEN (input.PmsRecallReasonId is null or input.PmsRecallReasonId ='') 
			THEN 
				(SELECT PmsRecallReasonId FROM [shadow].RecallReason ren WHERE TRIM(ren.ReasonName) = TRIM(input.ReasonName) and ren.PmsInstanceId = input.PmsInstanceId) 						
			ELSE 
				(SELECT PmsRecallReasonId FROM [shadow].RecallReason res WHERE TRIM(res.PmsRecallReasonId) = TRIM(input.PmsRecallReasonId) and res.PmsInstanceId = input.PmsInstanceId AND TRIM(res.PmsRecallReasonId) IS NOT NULL)
			END AS RecallReasonId,
		input.Deleted,
		input.UTCCreatedDate,
		input.UTCLastModifiedDate
	into #temp
	from @inputData input
	left outer join [shadow].Location loc  on TRIM(loc.PmsLocationId)  = TRIM(input.PmsLocationId) AND loc.PmsInstanceId  = input.PmsInstanceId
	left outer join [shadow].Provider prov on TRIM(prov.PmsProviderId) = TRIM(input.PmsProviderId) AND prov.PmsInstanceId = input.PmsInstanceId
	left outer join [shadow].Patient  pat  on TRIM(pat.PmsPatientID)   = TRIM(input.PmsPatientID)  AND pat.PmsInstanceId  = input.PmsInstanceId


	MERGE INTO [shadow].[Recall] as target
	USING #temp as source
	on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsRecallId) = TRIM(source.PmsRecallId)
	WHEN NOT MATCHED BY TARGET 
	THEN
		INSERT (PmsInstanceId, 
				PmsRecallId, 
				PmsPatientId,
				PmsLocationId,
				PmsProviderId,
				RecallDate,
				RecallReasonId,
				Deleted,
				UtcCreatedDate, 
				UtcLastModifiedDate)
		VALUES (source.PmsInstanceId, 
				TRIM(source.PmsRecallId), 
				ISNULL(TRIM(source.PmsPatientId),0),
				ISNULL(TRIM(source.PmsLocationId),0),
				ISNULL(TRIM(source.PmsProviderId),0),
				source.RecallDate,
				ISNULL(source.RecallReasonId,0),
				source.Deleted,
				@utcDate, 
				@utcDate)
	WHEN MATCHED AND
	(
		CHECKSUM
		(
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsPatientId,0))		+	
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsLocationId,0))		+
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsProviderId,0))		+
			ISNULL(CONVERT(VARCHAR(MAX),source.RecallDate),CONVERT(VARCHAR(MAX),''))	+
			CONVERT(VARCHAR(MAX), ISNULL(source.RecallReasonId,0))		+
			ISNULL(CONVERT(VARCHAR(MAX),source.Deleted),CONVERT(VARCHAR(MAX),''))
		)
		<>
		CHECKSUM
		(
			CONVERT(VARCHAR(MAX), target.PmsPatientId)		+	
			CONVERT(VARCHAR(MAX), target.PmsLocationId)		+
			CONVERT(VARCHAR(MAX), target.PmsProviderId)		+
			ISNULL(CONVERT(VARCHAR(MAX),target.RecallDate),CONVERT(VARCHAR(MAX),''))	+
			CONVERT(VARCHAR(MAX), target.RecallReasonId)		+
			ISNULL(CONVERT(VARCHAR(MAX),target.Deleted),CONVERT(VARCHAR(MAX),''))
		)
	)
	THEN
		UPDATE SET 
				target.PmsPatientID	= ISNULL(TRIM(source.PmsPatientId),0),
				target.PmsLocationID = ISNULL(TRIM(source.PmsLocationId),0),
				target.PmsProviderID = ISNULL(TRIM(source.PmsProviderId),0),
				target.RecallDate	= source.RecallDate,
				target.RecallReasonID = ISNULL(source.RecallReasonId,0),
				target.Deleted		= source.Deleted, 
				UtcLastModifiedDate = @utcDate
		OUTPUT   -- delta table
			source.PmsInstanceId,
			TRIM(source.PmsRecallId),
			@utcDate,
			CASE 
			WHEN $action = 'INSERT' THEN 1 
			WHEN $action = 'UPDATE' THEN 2
				END AS ActionPerformed
		INTO [shadow].[RecallDelta](PmsInstanceId, PmsRecallId, UtcCreatedDate, ActionPerformed); 
 
	drop table #temp;

	SET NOCOUNT OFF;
END;
GO

CREATE  PROCEDURE [shadow].[sp_ResourceUpsert]
	@paramUtcOffset int,
	@inputData shadow.udtt_Resource readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	MERGE INTO [shadow].[Resource] as target
	USING @inputData as source
    ON target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsResourceId) = TRIM(source.PmsResourceId)
	WHEN NOT MATCHED BY TARGET
	THEN
        INSERT (PmsInstanceId, PmsResourceId, ResourceName, UtcCreatedDate, UtcLastModifiedDate)
		VALUES (source.PmsInstanceId, TRIM(source.PmsResourceId), TRIM(source.ResourceName), @utcDate, @utcDate)
	WHEN MATCHED AND
	(
		CHECKSUM
		(
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ResourceName),CONVERT(VARCHAR(MAX),'')))
		)
		<>
		CHECKSUM
		(
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ResourceName),CONVERT(VARCHAR(MAX),'')))
		)
	)
	THEN
		UPDATE SET 
            ResourceName = TRIM(source.ResourceName),
            UtcLastModifiedDate = @utcDate
		OUTPUT   -- delta table
			source.PmsInstanceId,
			TRIM(source.PmsResourceId),
			@utcDate,
			CASE 
				WHEN $action = 'INSERT' THEN 1 
				WHEN $action = 'UPDATE' THEN 2
			 END AS ActionPerformed
		INTO [shadow].[ResourceDelta](PmsInstanceId, PmsResourceId, UtcCreatedDate, ActionPerformed); 
    
	SET NOCOUNT OFF;
END;
GO

CREATE PROCEDURE [shadow].[sp_TransactionMarkDeleted]
	@paramUtcOffset int,
	@inputData [shadow].udtt_PmsId readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	MERGE INTO [shadow].TransactionItem as target
	USING @inputData as source
	on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsTransactionId) = TRIM(source.PmsId)
	WHEN MATCHED THEN
	UPDATE 
	SET 
		Deleted  = 1,
		UtcLastModifiedDate = @utcDate
	OUTPUT   -- delta table
		source.PmsInstanceId,
		TRIM(source.PmsId),
		@utcDate,
		3 AS ActionPerformed
	INTO [shadow].[TransactionItemDelta](PmsInstanceId, PmsTransactionItemId, UtcCreatedDate, ActionPerformed); 
END
GO

CREATE PROCEDURE [shadow].[sp_VisitMarkDeleted]
	@paramUtcOffset int,
	@inputData [shadow].udtt_PmsId readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	MERGE INTO [shadow].Visit as target
	USING @inputData as source
	on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsVisitId) = TRIM(source.PmsId)
	WHEN MATCHED THEN
	UPDATE 
	SET 
		Deleted  = 1,
		UtcLastModifiedDate = @utcDate
	OUTPUT   -- delta table
		source.PmsInstanceId,
		TRIM(source.PmsId),
		@utcDate,
		3 AS ActionPerformed
	INTO [shadow].[VisitDelta](PmsInstanceId, PmsVisitId, UtcCreatedDate, ActionPerformed); 


END
GO

CREATE  PROCEDURE [shadow].[sp_VisitUpsert]
	@paramUtcOffset int,
	@inputData shadow.udtt_Visit readonly
AS
BEGIN

	DECLARE @utcOffset int =  @paramUtcOffset
	DECLARE @utcDate datetime = GetUTCDate()

	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;
	select 
		input.PmsInstanceId,
		input.PmsVisitId,
		pat.PmsPatientId,
		CASE WHEN (input.PmsLocationId is null or input.PmsLocationId='') THEN pat.PmsLocationId ELSE loc.PmsLocationId END AS PmsLocationId ,
		CASE WHEN (input.PmsProviderId is null or input.PmsProviderId='') THEN pat.PmsProviderId ELSE prov.PmsProviderId END AS PmsProviderId ,
		input.PmsAppointmentId,
		input.VisitDate,
		input.VisitType,
		input.VisitReason,
		input.ShowedUp,
		input.ICD1,
		input.ICD2,
		input.ICD3,
		input.CPT1,
		input.CPT2,
		input.CPT3,
		input.ProductType,
		input.ProductBrand1,
		input.ProductBrand2,
		input.ProductBrand3,
		input.SpendAmt,
		input.Deleted,
		input.PmsCreatedDate,
		input.PmsLastModifiedDate,
		input.UTCCreatedDate,
		input.UTCLastModifiedDate
	into #temp
	from @inputData input
	left outer join [shadow].Location loc  on TRIM(loc.PmsLocationId)  = TRIM(input.PmsLocationId) AND loc.PmsInstanceId  = input.PmsInstanceId
	left outer join [shadow].Provider prov on TRIM(prov.PmsProviderId) = TRIM(input.PmsProviderId) AND prov.PmsInstanceId = input.PmsInstanceId
	left outer join [shadow].Patient pat   on TRIM(pat.PmsPatientId)   = TRIM(input.PmsPatientId ) AND pat.PmsInstanceId  = input.PmsInstanceId
	--	left outer join [shadow].Appointment appt on appt.PmsAppointmentITRIM(d = input.PmsAppointmentId AND appt.PmsInstanceId = input.PmsInstanceId

	--  update patients with contact lens wearer information
	Merge into [shadow].Patient as target
	using (Select Distinct PmsInstanceId, PmsPatientId From #temp Where TRIM(ProductType)='Contact Lenses' Group By PmsInstanceId,PmsPatientId) as source
	on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsPatientId) = TRIM(source.PmsPatientId) 
	WHEN MATCHED THEN
		UPDATE
			set target.ContactLensWearer = 'Y',
				target.UtcLastModifiedDate = @utcDate;

	--  update patients with last exam visit info
	Merge into [shadow].Patient as target
	using (Select PmsInstanceId, PmsPatientId,Max(VisitDate) As VisitDate From #temp Where TRIM(VisitType)='Exam' and VisitDate is not null Group By PmsInstanceId,PmsPatientId) as source
	on target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsPatientId) = TRIM(source.PmsPatientId) and source.VisitDate > target.LastVisitOrExamDate 
	WHEN MATCHED THEN
		UPDATE
			set target.LastVisitOrExamDate = source.VisitDate,
				target.UtcLastModifiedDate = @utcDate;

	MERGE INTO [shadow].[Visit] as target
	USING @inputData as source
    ON target.PmsInstanceId = source.PmsInstanceId AND TRIM(target.PmsVisitId) = TRIM(source.PmsVisitId)
	WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (PmsInstanceId, 
				PmsVisitId, 
				PmsPatientId,
				PmsLocationId,
				PmsProviderId,
				PmsAppointmentId,
				VisitDate,
				VisitType,
				VisitReason,
				ShowedUp,
				ICD1,
				ICD2,
				ICD3,
				CPT1,
				CPT2,
				CPT3,
				ProductType,
				ProductBrand1,
				ProductBrand2,
				ProductBrand3,
				SpendAmt,
				Deleted,
				PmsCreatedDate,
				PmsLastModifiedDate,
				UtcCreatedDate, 
				UtcLastModifiedDate)
		VALUES (source.PmsInstanceId, 
				TRIM(source.PmsVisitId), 
				ISNULL(TRIM(source.PmsPatientId),0),
				ISNULL(TRIM(source.PmsLocationId),0),
				ISNULL(TRIM(source.PmsProviderId),0),
				TRIM(source.PmsAppointmentId),
				source.VisitDate,
				TRIM(source.VisitType),
				TRIM(source.VisitReason),
				source.ShowedUp,
				TRIM(source.ICD1),
				TRIM(source.ICD2),
				TRIM(source.ICD3),
				TRIM(source.CPT1),
				TRIM(source.CPT2),
				TRIM(source.CPT3),
				TRIM(source.ProductType),
				TRIM(source.ProductBrand1),
				TRIM(source.ProductBrand2),
				TRIM(source.ProductBrand3),
				source.SpendAmt,
				source.Deleted,
				source.PmsCreatedDate,
				source.PmsLastModifiedDate,
				@utcDate, 
				@utcDate )
	WHEN MATCHED AND
	(
		CHECKSUM
		(
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsLocationId,0))	+
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsProviderId,0))	+
			CONVERT(VARCHAR(MAX), ISNULL(source.PmsPatientId,0))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.PmsAppointmentId),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),(CASE WHEN source.VisitDate > target.VisitDate THEN source.VisitDate ELSE target.VisitDate END)),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.VisitType),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.VisitReason),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.ShowedUp),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ICD1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ICD2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ICD3),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.CPT1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.CPT2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.CPT3),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ProductType),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ProductBrand1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ProductBrand2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),source.ProductBrand3),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.SpendAmt),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.Deleted),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PMSCreatedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),source.PMSLastModifiedDate),CONVERT(VARCHAR(MAX),''))
		)
		<>
		CHECKSUM
		(
			CONVERT(VARCHAR(MAX), target.PmsLocationId)	+
			CONVERT(VARCHAR(MAX), target.PmsProviderId)	+
			CONVERT(VARCHAR(MAX), target.PmsPatientId)	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.PmsAppointmentId),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.VisitDate),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.VisitType),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.VisitReason),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.ShowedUp),CONVERT(VARCHAR(MAX),''))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ICD1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ICD2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ICD3),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.CPT1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.CPT2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.CPT3),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ProductType),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ProductBrand1),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ProductBrand2),CONVERT(VARCHAR(MAX),'')))	+
			TRIM(ISNULL(CONVERT(VARCHAR(MAX),target.ProductBrand3),CONVERT(VARCHAR(MAX),'')))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.SpendAmt),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.Deleted),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PMSCreatedDate),CONVERT(VARCHAR(MAX),''))	+
			ISNULL(CONVERT(VARCHAR(MAX),target.PMSLastModifiedDate),CONVERT(VARCHAR(MAX),''))
		)
	)
	THEN
		UPDATE SET 
				target.PmsPatientId =  ISNULL(TRIM(source.PmsPatientId),0),
				target.PmsLocationId = ISNULL(TRIM(source.PmsLocationId),0),
				target.PmsProviderId = ISNULL(TRIM(source.PmsProviderId),0),
				target.PmsAppointmentId = TRIM(source.PmsAppointmentId),
				target.VisitDate = CASE WHEN source.VisitDate > target.VisitDate THEN source.VisitDate ELSE target.VisitDate END,
				target.VisitType = TRIM(source.VisitType),
				target.VisitReason = TRIM(source.VisitReason),
				target.ShowedUp = source.ShowedUp,
				target.ICD1 = TRIM(source.ICD1),
				target.ICD2 = TRIM(source.ICD2),
				target.ICD3 = TRIM(source.ICD3),
				target.CPT1 = TRIM(source.CPT1),
				target.CPT2 = TRIM(source.CPT2),
				target.CPT3 = TRIM(source.CPT3),
				target.ProductType = TRIM(source.ProductType),
				target.ProductBrand1 = TRIM(source.ProductBrand1),
				target.ProductBrand2 = TRIM(source.ProductBrand2),
				target.ProductBrand3 = TRIM(source.ProductBrand3),
				target.SpendAmt = source.SpendAmt,
				target.Deleted = source.Deleted, 
				target.PmsCreatedDate = source.PmsCreatedDate,
				target.PmsLastModifiedDate = source.PmsLastModifiedDate,
				UtcLastModifiedDate = @utcDate
		OUTPUT   -- delta table
			source.PmsInstanceId,
			TRIM(source.PmsVisitId),
			@utcDate,
			CASE 
				WHEN $action = 'INSERT' THEN 1 
				WHEN $action = 'UPDATE' THEN 2
			 END AS ActionPerformed
		INTO [shadow].[VisitDelta](PmsInstanceId, PmsVisitId, UtcCreatedDate, ActionPerformed); 
    
	DROP table #temp;

	SET NOCOUNT OFF;
END;
GO
