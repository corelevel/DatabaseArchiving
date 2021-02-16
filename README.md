# DatabaseArchiving
PowerShell script can be used for data archiving and data purging. It automatically creates a table script for the destination database (if the table not exists) and if a new column added to the source it adds that column to the destination. Also, the script supports resume after failure.

Archiving and purging process performed by groups. Each group has a set of tables and settings, and each table has its own additional settings.

For each group following settings exists:
* *Name* - unique group name
* *SrcServerName* - source server name
* *SrcDatabaseName* - source database name
* *SrcConnectionOptions* - source connection string options such as ApplicationIntent, Connect Timeout, user name, password, etc
* *DstServerName* - destination server name
* *DstDatabaseName* - destination database name
* *DstConnectionOptions* - destination connection string options such as ApplicationIntent, Connect Timeout, user name, password, etc
* *DisableFK* - disable foreign keys before the purge from the source database

For each table in a group following settings exists:
* *SchemaName* - source table schema name
* *TableName* - source table name
* *Active* - process this table or not
* *DataCopyBatchSize* - batch size for data copy
* *KeyCopyBatchSize*	- batch size for keys copy
* *KeyQuery*	- to select primary keys values from source. For example, for daily archiving, you can use: *select OrderId from dbo.Order where OrderDate >= dateadd(day, -1, cast(getdate() as date)) and OrderDate < cast(getdate() as date)*
* *Archive*	- can be archived or not
* *Purge*	- can be purged or not
* *PurgeOrder*	- used to get purge sequence to avoid reference integrity errors
* *DelayInterval*	- delay interval between batches in format 'hh:mm:ss'
* *AlwaysRunCheck* - always check for previously copied records
* *SrcWorkingTableName* - working table name for source primary keys (computed column)
* *DstWorkingTableName* - working table name for destination primary keys (computed column)
* *WorkingTableKeyName* - working table primary key column name (computed column)
* *WorkingTableFlagName* - 0 - ok, 1 - means row is duplicate and must be skipped (computed column)

When archiving/purging process starts it creates a state record with the following data:
* *CreateDate* - record create date 
* *KeyCopyDate* - date when primary keys were copied successfully
* *KeyMaxValue* - max value for the surrogate primary in *SrcWorkingTableName*
* *LastArchivedKey*	- last successfully copied surrogate primary key
* *LastArchivedDate* - date of the last successful data copy
* *RowsCopied* - total rows copied
* *LastPurgedKey*	- last successfully purged surrogate primary key
* *LastPurgedDate* - date of the last successful data purge
* *RowsPurged* - total rows purged
* *CompleteDate* - successful completion date

The working database must be created on the source database server. Schema contained in *schema.sql* file.
