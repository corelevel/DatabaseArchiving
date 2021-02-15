# DatabaseArchiving
 
This PowerShell script can be used for data archiving and data purging.

Archiving and purging process performed by groups. Each group has a set of tables and settings.

For each group, we have the following settings:
Name - unique group name
SrcServerName - source server name
SrcDatabaseName - source database name
SrcConnectionOptions - source connection string options such as ApplicationIntent, Connect Timeout, user name, password, etc
DstServerName - destination server name
DstDatabaseName - destination database name
DstConnectionOptions - destination connection string options such as ApplicationIntent, Connect Timeout, user name, password, etc
DisableFK - disable foreign keys before the purge from the source database

For each table in group, we have the following settings:
SchemaName - source table schema name
TableName - source table name
Active - process this table or not
DataCopyBatchSize - batch size for data copy
KeyCopyBatchSize	- batch size for keys copy
KeyQuery	- to select primary keys values from source. for example: select OrderId from dbo.Order where OrderDate >= dateadd(day, -1, cast(getdate() as date)) and OrderDate < cast(getdate() as date)
Archive	- can be archived or not
Purge	- can be purged or not
PurgeOrder	- used to get purge sequence to avoid reference integrity errors
DelayInterval	- delay interval between batches in format 'hh:mm:ss'
AlwaysRunCheck - always check for previously copied records
SrcWorkingTableName - working table name for source primary keys (computed column)
DstWorkingTableName - working table name for destination primary keys (computed column)
WorkingTableKeyName - working table primary key column name (computed column)
WorkingTableFlagName - 0 - ok, 1 - means row is duplicate and must be skipped (computed column)
