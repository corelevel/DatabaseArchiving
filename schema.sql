if not exists (
	select	1
	from	INFORMATION_SCHEMA.TABLES
	where TABLE_NAME = 'TableGroup' and TABLE_SCHEMA ='dbo' and TABLE_TYPE = 'BASE TABLE')
begin
	create table dbo.TableGroup
	(
		TableGroupId			int identity(1,1) not null,
		[Name]					sysname not null,

		SrcServerName			sysname not null,	-- source server name
		SrcDatabaseName			sysname not null,	-- source database name
		SrcConnectionOptions	nvarchar(max) null,	-- connection string options like ApplicationIntent, time out, user name, password etc

		DstServerName			sysname not null,	-- destination server name
		DstDatabaseName			sysname not null,	-- destination database name
		DstConnectionOptions	nvarchar(max) null,	-- connection string options like ApplicationIntent, time out, user name, password etc

		DisableFK				bit not null		-- disable FK before purge
	)

	alter table dbo.TableGroup add constraint PK_TableGroup primary key clustered (TableGroupId)
	alter table dbo.TableGroup add constraint UQ_TableGroup__Name unique ([Name])
end
go

if not exists (
	select	1
	from	INFORMATION_SCHEMA.TABLES
	where TABLE_NAME = 'SourceTable' and TABLE_SCHEMA ='dbo' and TABLE_TYPE = 'BASE TABLE')
begin
	create table dbo.SourceTable
	(
		SourceTableId		int identity(1,1) not null,
		TableGroupId		int not null,

		SchemaName			sysname not null,
		TableName			sysname not null,

		Active				bit not null,

		DataCopyBatchSize	int not null,			-- batch size for data copy
		KeyCopyBatchSize	int not null,			-- batch size for keys copy
		KeyQuery			nvarchar(max) not null,	-- to select primary keys values from source

		Archive				bit not null,			-- can be archived

		Purge				bit not null,			-- can be purged
		PurgeOrder			smallint not null,		-- used to get purge sequence

		DelayInterval		char(8) not null,		-- 'hh:mm:ss'

		AlwaysRunCheck	bit not null,			-- always check for previously copied records

		SrcWorkingTableName as SchemaName + '_' + TableName + '__src',	-- working table name for source primary keys
		DstWorkingTableName as SchemaName + '_' + TableName + '__dst',	-- working table name for destination primary keys
		WorkingTableKeyName as TableName + '__key',
		WorkingTableFlagName as TableName + '__skip'	-- 0 - ok, 1 - means row is duplicate and must be skipped
	)

	alter table dbo.SourceTable add constraint PK_SourceTable primary key clustered (SourceTableId)
	alter table dbo.SourceTable add constraint UQ_SourceTable__TableGroupId_SchemaName_TableName unique (TableGroupId, SchemaName, TableName)
	create nonclustered index IX_SourceTable__TableGroupId on dbo.SourceTable(TableGroupId)
	alter table dbo.SourceTable add constraint FK_SourceTable_TableGroup__TableGroupId foreign key (TableGroupId)
	references dbo.TableGroup (TableGroupId)
end
go

if not exists (
	select	1
	from	INFORMATION_SCHEMA.TABLES
	where TABLE_NAME = 'ProcessState' and TABLE_SCHEMA ='dbo' and TABLE_TYPE = 'BASE TABLE')
begin
	create table dbo.ProcessState
	(
		ProcessStateId		bigint identity(1,1) not null,
		SourceTableId		int not null,
		CreateDate			datetime not null,

		KeyCopyDate			datetime null,		-- date of primary keys copy

		KeyMaxValue			int null,

		LastArchivedKey		int null,			-- last copied surrogate primary key
		LastArchivedDate	datetime null,		-- date of the last copy
		RowsCopied			int null,			-- rows count

		LastPurgedKey		int null,			-- last purged surrogate primary key
		LastPurgedDate		datetime null,		-- date of the last purge
		RowsPurged			int null,			-- rows count

		CompleteDate		datetime null
	)

	alter table dbo.ProcessState add constraint PK_ProcessState primary key clustered (ProcessStateId)
	create nonclustered index IXF_ProcessState__SourceTableId ON dbo.ProcessState(SourceTableId) where CompleteDate is null
	create nonclustered index IX_ProcessState__SourceTableId ON dbo.ProcessState(SourceTableId)
	alter table dbo.ProcessState add constraint FK_ProcessState_SourceTable__SourceTableId foreign key (SourceTableId)
	references dbo.SourceTable (SourceTableId)
end
go

if not exists (select 1 from INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = 'stp_InsertProcessState' and ROUTINE_SCHEMA = 'dbo' and ROUTINE_TYPE = 'PROCEDURE')
begin
	exec sp_executesql N'create procedure dbo.stp_InsertProcessState as select ''Fake procedure to be replaced by alter script'''
end
go
alter procedure dbo.stp_InsertProcessState
	@SourceTableId	int
as
set nocount on

insert dbo.ProcessState(SourceTableId, CreateDate)
values (@SourceTableId, getdate())

select cast(scope_identity() as bigint) ProcessStateId
go

if not exists (select 1 from INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = 'stp_GetTableGroup' and ROUTINE_SCHEMA = 'dbo' and ROUTINE_TYPE = 'PROCEDURE')
begin
	exec sp_executesql N'create procedure dbo.stp_GetTableGroup as select ''Fake procedure to be replaced by alter script'''
end
go
alter procedure dbo.stp_GetTableGroup
	@Name	sysname
as
set nocount on

select	TableGroupId,
		SrcServerName,
		SrcDatabaseName,
		SrcConnectionOptions,
		DstServerName,
		DstDatabaseName,
		DstConnectionOptions,
		DisableFK
from	dbo.TableGroup
where [Name] = @Name
go

if not exists (select 1 from INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = 'stp_GetSourceTable' and ROUTINE_SCHEMA = 'dbo' and ROUTINE_TYPE = 'PROCEDURE')
begin
	exec sp_executesql N'create procedure dbo.stp_GetSourceTable as select ''Fake procedure to be replaced by alter script'''
end
go
alter procedure dbo.stp_GetSourceTable
	@TableGroupId	int
as
set nocount on

select	SourceTableId,
		SchemaName,
		TableName,
		DataCopyBatchSize,
		KeyCopyBatchSize,
		KeyQuery,
		Archive,
		Purge,
		PurgeOrder,
		DelayInterval,
		AlwaysRunCheck,
		SrcWorkingTableName,
		DstWorkingTableName,
		WorkingTableKeyName,
		WorkingTableFlagName
from	dbo.SourceTable
where Active = 1 and TableGroupId = @TableGroupId
go

if not exists (select 1 from INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = 'stp_GetIncompleteProcessState' and ROUTINE_SCHEMA = 'dbo' and ROUTINE_TYPE = 'PROCEDURE')
begin
	exec sp_executesql N'create procedure dbo.stp_GetIncompleteProcessState as select ''Fake procedure to be replaced by alter script'''
end
go
alter procedure dbo.stp_GetIncompleteProcessState
	@SourceTableId	int
as
set nocount on

select top 1 ProcessStateId,
		CreateDate,
		KeyCopyDate,
		KeyMaxValue,
		LastArchivedKey,
		LastArchivedDate,
		RowsCopied,
		LastPurgedKey,
		LastPurgedDate,
		RowsPurged
from	dbo.ProcessState
where SourceTableId = @SourceTableId and CompleteDate is null
order by ProcessStateId desc
go

if not exists (select 1 from INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = 'stp_UpdateProcessState' and ROUTINE_SCHEMA = 'dbo' and ROUTINE_TYPE = 'PROCEDURE')
begin
	exec sp_executesql N'create procedure dbo.stp_UpdateProcessState as select ''Fake procedure to be replaced by alter script'''
end
go
alter procedure dbo.stp_UpdateProcessState
	@ProcessStateId		bigint,
	@KeyCopyDate		datetime = null,
	@KeyMaxValue		int = null,
	@LastArchivedKey	int = null,
	@LastArchivedDate	datetime = null,
	@RowsCopied			int = null,
	@LastPurgedKey		int = null,
	@LastPurgedDate		datetime = null,
	@RowsPurged			int = null,
	@CompleteDate		datetime = null
as
set nocount on

update	dbo.ProcessState
set		KeyCopyDate = isnull(@KeyCopyDate, KeyCopyDate),
		KeyMaxValue = isnull(@KeyMaxValue, KeyMaxValue),
		LastArchivedKey = isnull(@LastArchivedKey, LastArchivedKey),
		LastArchivedDate = isnull(@LastArchivedDate, LastArchivedDate),
		RowsCopied = isnull(@RowsCopied, RowsCopied),
		LastPurgedKey = isnull(@LastPurgedKey, LastPurgedKey),
		LastPurgedDate = isnull(@LastPurgedDate, LastPurgedDate),
		RowsPurged = isnull(@RowsPurged, RowsPurged),
		CompleteDate = isnull(@CompleteDate, CompleteDate)
where ProcessStateId = @ProcessStateId
go

if not exists (select 1 from INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = 'stp_GetBulkCopyData' and ROUTINE_SCHEMA = 'dbo' and ROUTINE_TYPE = 'PROCEDURE')
begin
	exec sp_executesql N'create procedure dbo.stp_GetBulkCopyData as select ''Fake procedure to be replaced by alter script'''
end
go
alter procedure dbo.stp_GetBulkCopyData
	@ProcessStateId	bigint
as
set nocount on

declare @SrcDatabaseName sysname, @SchemaName sysname, @TableName sysname, @SrcWorkingTableName sysname
declare @WorkingTableKeyName sysname, @WorkingTableFlagName sysname
declare @DataCopyBatchSize int, @LastArchivedKey int, @Query nvarchar(max)
declare @SelectColumns nvarchar(max), @JoinColumns nvarchar(max)

select	@SrcDatabaseName = gr.SrcDatabaseName,
		@SchemaName = ta.SchemaName,
		@TableName = ta.TableName,
		@DataCopyBatchSize = ta.[DataCopyBatchSize],
		@SrcWorkingTableName = ta.SrcWorkingTableName,
		@WorkingTableKeyName = ta.WorkingTableKeyName,
		@WorkingTableFlagName = ta.WorkingTableFlagName,
		@LastArchivedKey = isnull(st.LastArchivedKey, 0)
from	dbo.ProcessState st
		join dbo.SourceTable ta
		on ta.SourceTableId = st.SourceTableId
		join dbo.TableGroup gr
		on gr.TableGroupId = ta.TableGroupId
where st.ProcessStateId = @ProcessStateId

-- get table columns, without computed columns
set @Query = 'set @SelectColumns = (
	select	''so.['' + COLUMN_NAME + ''], ''
	from	[' + @SrcDatabaseName + '].INFORMATION_SCHEMA.COLUMNS co
	where TABLE_SCHEMA = ''' + @SchemaName + ''' and TABLE_NAME = ''' + @TableName + '''
		and not exists
		(
		select	1
		from	[' + @SrcDatabaseName + '].sys.columns sc
		where sc.[object_id] = object_id(''' + @SrcDatabaseName + '.'' + co.TABLE_SCHEMA + ''.'' + co.TABLE_NAME) and sc.[name] = co.COLUMN_NAME and sc.is_computed = 1
		)
	for xml path('''')
)'
exec sp_executesql @Query, N'@SelectColumns nvarchar(max) output', @SelectColumns = @SelectColumns output
set @SelectColumns = left(@SelectColumns, len(@SelectColumns) - 1)

-- get primary key columns for join
set @Query = 'set @JoinColumns = (
	select	''wo.['' + ccu.COLUMN_NAME + ''] = so.['' + ccu.COLUMN_NAME + ''] and ''
	from	[' + @SrcDatabaseName + '].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
			join [' + @SrcDatabaseName + '].INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
			on ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME and ccu.TABLE_NAME = tc.TABLE_NAME and ccu.TABLE_SCHEMA = tc.TABLE_SCHEMA
	where tc.TABLE_SCHEMA = ''' + @SchemaName + ''' and tc.TABLE_NAME = ''' + @TableName + '''
		and tc.CONSTRAINT_TYPE = ''PRIMARY KEY''
	for xml path('''')
)'
exec sp_executesql @Query, N'@JoinColumns nvarchar(max) output', @JoinColumns = @JoinColumns output
set @JoinColumns = left(@JoinColumns, len(@JoinColumns) - 4)

set @Query = 'select ' + @SelectColumns + '
from	[' + @SrcDatabaseName + '].[' + @SchemaName + '].[' + @TableName + '] so
		join
		(
		select top(@DataCopyBatchSize) t.*
		from	dbo.[' + @SrcWorkingTableName + '] t
		where t.[' + @WorkingTableKeyName + '] > @LastArchivedKey and [' + @WorkingTableFlagName + '] = 0
		order by t.[' + @WorkingTableKeyName + ']
		) wo
		on ' + @JoinColumns

exec sp_executesql @Query, N'@DataCopyBatchSize int, @LastArchivedKey int', @DataCopyBatchSize = @DataCopyBatchSize, @LastArchivedKey = @LastArchivedKey
go

if not exists (select 1 from INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = 'stp_PurgeData' and ROUTINE_SCHEMA = 'dbo' and ROUTINE_TYPE = 'PROCEDURE')
begin
	exec sp_executesql N'create procedure dbo.stp_PurgeData as select ''Fake procedure to be replaced by alter script'''
end
go
alter procedure dbo.stp_PurgeData
	@ProcessStateId	bigint
as
set nocount on

declare @SrcDatabaseName sysname, @SchemaName sysname, @TableName sysname, @SrcWorkingTableName sysname
declare @WorkingTableKeyName sysname, @WorkingTableFlagName sysname
declare @DataCopyBatchSize int, @LastPurgedKey int, @Query nvarchar(max)
declare @JoinColumns nvarchar(max)

select	@SrcDatabaseName = gr.SrcDatabaseName,
		@SchemaName = ta.SchemaName,
		@TableName = ta.TableName,
		@DataCopyBatchSize = ta.[DataCopyBatchSize],
		@SrcWorkingTableName = ta.SrcWorkingTableName,
		@WorkingTableKeyName = ta.WorkingTableKeyName,
		@WorkingTableFlagName = ta.WorkingTableFlagName,
		@LastPurgedKey = isnull(st.LastPurgedKey, 0)
from	dbo.ProcessState st
		join dbo.SourceTable ta
		on ta.SourceTableId = st.SourceTableId
		join dbo.TableGroup gr
		on gr.TableGroupId = ta.TableGroupId
where st.ProcessStateId = @ProcessStateId

-- get primary key columns for join
set @Query = 'set @JoinColumns = (
	select	''so.['' + ccu.COLUMN_NAME + ''] = wo.['' + ccu.COLUMN_NAME + ''] and ''
	from	[' + @SrcDatabaseName + '].INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
			join [' + @SrcDatabaseName + '].INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
			on ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME and ccu.TABLE_NAME = tc.TABLE_NAME and ccu.TABLE_SCHEMA = tc.TABLE_SCHEMA
	where tc.TABLE_SCHEMA = ''' + @SchemaName + ''' and tc.TABLE_NAME = ''' + @TableName + '''
		and tc.CONSTRAINT_TYPE = ''PRIMARY KEY''
	for xml path('''')
)'
exec sp_executesql @Query, N'@JoinColumns nvarchar(max) output', @JoinColumns = @JoinColumns output
set @JoinColumns = left(@JoinColumns, len(@JoinColumns) - 4)

set @Query = 'delete so
from	[' + @SrcDatabaseName + '].[' + @SchemaName + '].[' + @TableName + '] so
		join
		(
		select top(@DataCopyBatchSize) t.*
		from	dbo.[' + @SrcWorkingTableName + '] t
		where t.[' + @WorkingTableKeyName + '] > @LastPurgedKey
		order by t.[' + @WorkingTableKeyName + ']
		) wo
		on ' + @JoinColumns + '

select @@rowcount RowsPurgedForBatch'

exec sp_executesql @Query, N'@DataCopyBatchSize int, @LastPurgedKey int', @DataCopyBatchSize = @DataCopyBatchSize, @LastPurgedKey = @LastPurgedKey
go

if not exists (select 1 from INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = 'stp_UpdateKeyMaxValue' and ROUTINE_SCHEMA = 'dbo' and ROUTINE_TYPE = 'PROCEDURE')
begin
	exec sp_executesql N'create procedure dbo.stp_UpdateKeyMaxValue as select ''Fake procedure to be replaced by alter script'''
end
go
alter procedure dbo.stp_UpdateKeyMaxValue
	@ProcessStateId	bigint
as
set nocount on

declare @SchemaName sysname, @TableName sysname, @SrcWorkingTableName sysname
declare @WorkingTableKeyName sysname, @KeyMaxValue int
declare @Query nvarchar(max)

select	@SchemaName = ta.SchemaName,
		@TableName = ta.TableName,
		@SrcWorkingTableName = ta.SrcWorkingTableName,
		@WorkingTableKeyName = ta.WorkingTableKeyName
from	dbo.ProcessState ast
		join dbo.SourceTable ta
		on ta.SourceTableId = ast.SourceTableId
		join dbo.TableGroup gr
		on gr.TableGroupId = ta.TableGroupId
where ast.ProcessStateId = @ProcessStateId

set @Query = 'select @KeyMaxValue = isnull(max([' + @WorkingTableKeyName + ']), 0) from dbo.[' + @SrcWorkingTableName + ']'

exec sp_executesql @Query, N'@KeyMaxValue int output', @KeyMaxValue = @KeyMaxValue output

exec dbo.stp_UpdateProcessState @ProcessStateId = @ProcessStateId, @KeyMaxValue = @KeyMaxValue

select @KeyMaxValue KeyMaxValue
go

if not exists (select 1 from INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = 'stp_FixLastArchivedKey' and ROUTINE_SCHEMA = 'dbo' and ROUTINE_TYPE = 'PROCEDURE')
begin
	exec sp_executesql N'create procedure dbo.stp_FixLastArchivedKey as select ''Fake procedure to be replaced by alter script'''
end
go
alter procedure dbo.stp_FixLastArchivedKey
	@ProcessStateId	bigint
as
set nocount on

declare @SchemaName sysname, @TableName sysname, @SrcWorkingTableName sysname, @DstWorkingTableName sysname
declare @WorkingTableKeyName sysname, @WorkingTableFlagName sysname, @LastArchivedKey bigint, @KeyMaxValue int
declare @Query nvarchar(max), @JoinColumns nvarchar(max)

select	@SchemaName = ta.SchemaName,
		@TableName = ta.TableName,
		@SrcWorkingTableName = ta.SrcWorkingTableName,
		@DstWorkingTableName = ta.DstWorkingTableName,
		@WorkingTableKeyName = ta.WorkingTableKeyName,
		@WorkingTableFlagName = ta.WorkingTableFlagName,
		@KeyMaxValue = ast.KeyMaxValue
from	dbo.ProcessState ast
		join dbo.SourceTable ta
		on ta.SourceTableId = ast.SourceTableId
where ast.ProcessStateId = @ProcessStateId

set @Query = 'set @JoinColumns = (
	select	''de.['' + co.COLUMN_NAME + ''] = so.['' + co.COLUMN_NAME + ''] and ''
	from	INFORMATION_SCHEMA.COLUMNS co
	where TABLE_SCHEMA = ''dbo'' and TABLE_NAME = ''' + @SrcWorkingTableName + '''
		and co.COLUMN_NAME not in (''' + @WorkingTableKeyName + ''', ''' + @WorkingTableFlagName + ''')
	for xml path('''')
)'
exec sp_executesql @Query, N'@JoinColumns nvarchar(max) output', @JoinColumns = @JoinColumns output
set @JoinColumns = left(@JoinColumns, len(@JoinColumns) - 4)

set @Query = 'update	so
set		[' + @WorkingTableFlagName + '] = 1
from	dbo.[' + @SrcWorkingTableName + '] so
		join dbo.[' + @DstWorkingTableName + '] de
		on ' + @JoinColumns + '

select	@LastArchivedKey = min([' + @WorkingTableKeyName + ']) - 1
from	dbo.[' + @SrcWorkingTableName + '] so
where [' + @WorkingTableFlagName + '] = 0'
exec sp_executesql @Query, N'@LastArchivedKey bigint output', @LastArchivedKey = @LastArchivedKey output

set @LastArchivedKey = isnull(@LastArchivedKey, @KeyMaxValue)

exec dbo.stp_UpdateProcessState @ProcessStateId = @ProcessStateId, @LastArchivedKey = @LastArchivedKey

select @LastArchivedKey LastArchivedKey
go

if not exists (select 1 from INFORMATION_SCHEMA.ROUTINES where ROUTINE_NAME = 'stp_DisableEnableFK' and ROUTINE_SCHEMA = 'dbo' and ROUTINE_TYPE = 'PROCEDURE')
begin
	exec sp_executesql N'create procedure dbo.stp_DisableEnableFK as select ''Fake procedure to be replaced by alter script'''
end
go
alter procedure dbo.stp_DisableEnableFK
	@ProcessStateId	bigint,
	@Disable		bit
as
set nocount on

declare @SrcDatabaseName sysname, @SchemaName sysname, @TableName sysname
declare @Query nvarchar(max), @AlterFK nvarchar(max)

select	@SrcDatabaseName = gr.SrcDatabaseName,
		@SchemaName = ta.SchemaName,
		@TableName = ta.TableName
from	dbo.ProcessState st
		join dbo.SourceTable ta
		on ta.SourceTableId = st.SourceTableId
		join dbo.TableGroup gr
		on gr.TableGroupId = ta.TableGroupId
where st.ProcessStateId = @ProcessStateId

set @Query = 'set @AlterFK = (
select	case	when @Disable = 1 then ''alter table [' + @SrcDatabaseName + '].['' + pa.TABLE_SCHEMA + ''].['' + pa.TABLE_NAME + ''] nocheck constraint ['' + pa.CONSTRAINT_NAME + ''];''
				else ''alter table [' + @SrcDatabaseName + '].['' + pa.TABLE_SCHEMA + ''].['' + pa.TABLE_NAME + ''] with nocheck check constraint ['' + pa.CONSTRAINT_NAME + ''];''
		end
from	[' + @SrcDatabaseName + '].INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS fk
		join [' + @SrcDatabaseName + '].INFORMATION_SCHEMA.KEY_COLUMN_USAGE pa
		on pa.CONSTRAINT_SCHEMA = fk.CONSTRAINT_SCHEMA and pa.CONSTRAINT_NAME = fk.CONSTRAINT_NAME 
		join [' + @SrcDatabaseName + '].INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS ref
		on ref.CONSTRAINT_SCHEMA = fk.UNIQUE_CONSTRAINT_SCHEMA and ref.CONSTRAINT_NAME = fk.UNIQUE_CONSTRAINT_NAME 
			and ref.ORDINAL_POSITION = pa.ORDINAL_POSITION
where ref.TABLE_SCHEMA = ''' + @SchemaName + ''' and ref.TABLE_NAME = ''' + @TableName + '''
for xml path('''')
)'

exec sp_executesql @Query, N'@AlterFK nvarchar(max) output, @Disable bit', @AlterFK = @AlterFK output, @Disable = @Disable

if @AlterFK is not null
begin
	exec sp_executesql @AlterFK
end
go
