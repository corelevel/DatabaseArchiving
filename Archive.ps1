# to install use command: Install-Module -Name SqlServer
Import-Module -Name SqlServer

# load .NET assembly
[System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.SMO') | out-null

class TableGroup
{
    [int]$Id
    [string]$Name

    [string]$SrcServerName
    [string]$SrcDatabaseName

    [string]$DstServerName
    [string]$DstDatabaseName

    [bool]$DisableFK

    [System.Data.SqlClient.SqlConnection]$ArcSqlConnection = [System.Data.SqlClient.SQLConnection]::new()
    [System.Data.SqlClient.SqlConnection]$SrcSqlConnection = [System.Data.SqlClient.SQLConnection]::new()
    [System.Data.SqlClient.SqlConnection]$DstSqlConnection = [System.Data.SqlClient.SQLConnection]::new()

    TableGroup([string]$connectionString, [string]$groupName)
    {
        $this.ArcSqlConnection.ConnectionString = $connectionString
        $this.ArcSqlConnection.Open()

        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new('dbo.stp_GetTableGroup', $this.ArcSqlConnection) 
        $sqlCommand.CommandType = [System.Data.CommandType]'StoredProcedure'
        $pName = $sqlCommand.Parameters.Add('@Name', [string])
        $pName.Value = $groupName

        [System.Data.SqlClient.SqlDataReader]$sqlReader = $sqlCommand.ExecuteReader()
        
        if ($sqlReader.Read())
        {
            $this.Id = $sqlReader['TableGroupId']
            $this.Name = $groupName

            $this.SrcServerName = $sqlReader['SrcServerName']
            $this.SrcDatabaseName = $sqlReader['SrcDatabaseName']

            $sb = [System.Data.Common.DbConnectionStringBuilder]::new()
            $sb.Add("Data Source", $this.SrcServerName)
            $sb.Add("Initial Catalog", $this.SrcDatabaseName)

            $this.SrcSqlConnection.ConnectionString = $sb.ConnectionString + ';' + $sqlReader['SrcConnectionOptions']
            $this.SrcSqlConnection.Open()

            $this.DstServerName = $sqlReader['DstServerName']
            $this.DstDatabaseName = $sqlReader['DstDatabaseName']

            $sb = [System.Data.Common.DbConnectionStringBuilder]::new()
            $sb.Add("Data Source", $this.DstServerName)
            $sb.Add("Initial Catalog", $this.DstDatabaseName)

            $this.DstSqlConnection.ConnectionString = $sb.ConnectionString + ';' + $sqlReader['DstConnectionOptions']
            $this.DstSqlConnection.Open()

            $this.DisableFK = $sqlReader['DisableFK']
        }
        $sqlReader.Close()
    }

    [System.Collections.ArrayList]GetSourceTables()
    {
        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new('dbo.stp_GetSourceTable', $this.ArcSqlConnection) 
        $sqlCommand.CommandType = [System.Data.CommandType]'StoredProcedure'
        $pTableGroupId = $sqlCommand.Parameters.Add('@TableGroupId', [int])
        $pTableGroupId.Value = $this.Id

        [System.Data.SqlClient.SqlDataReader]$sqlReader = $sqlCommand.ExecuteReader()
        
        [System.Collections.ArrayList]$tables = [System.Collections.ArrayList]::new()

        if (-not $sqlReader.HasRows)
        {
            $sqlReader.Close()
            return $tables
        }

        while ($sqlReader.Read())
        {
            $table = [SourceTable]::new()

            $table.Id = $sqlReader['SourceTableId']
            $table.SchemaName = $sqlReader['SchemaName']
            $table.TableName = $sqlReader['TableName']
            $table.DataCopyBatchSize = $sqlReader['DataCopyBatchSize']
            $table.PKCopyBatchSize = $sqlReader['PKCopyBatchSize']
            $table.KeyQuery = $sqlReader['KeyQuery']
            $table.Archive = $sqlReader['Archive']
            $table.Purge = $sqlReader['Purge']
            $table.DeleteOrder = $sqlReader['DeleteOrder']
            $table.DelayIntervalInSeconds = Get-DelayIntervalInSeconds $sqlReader['DelayInterval']
            $table.AlwaysRunCheck = $sqlReader['AlwaysRunCheck']
            $table.SrcWorkingTableName = $sqlReader['SrcWorkingTableName']
            $table.DstWorkingTableName = $sqlReader['DstWorkingTableName']
            $table.WorkingTableKeyName = $sqlReader['WorkingTableKeyName']
            $table.WorkingTableFlagName = $sqlReader['WorkingTableFlagName']
            $table.Group = $this

            [void]$tables.Add($table)
        }
        $sqlReader.Close()

        return $tables
    }
}

class Column
{
    [string]$Name
    [string]$DataType
    [string]$Collation
    [bool]$PrimaryKey
    [bool]$Computed
}

class ProcessState
{
    [long]$Id
    [int]$SourceTableId
    [DateTime]$CreateDate

    [DateTime]$KeyCopyDate
    [int]$KeyMaxValue

    [int]$LastArchivedKey
    [DateTime]$LastArchivedDate
    [int]$RowsCopied

    [int]$LastPurgedKey
    [DateTime]$LastPurgedDate
    [int]$RowsPurged

    [DateTime]$CompleteDate

    [bool]$IncompleteProcess

    [int]$RowsCopiedForBatch
    [int]$RowsPurgedForBatch

    [TableGroup]$Group

    UpdateKeyMaxValue()
    {
        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new('dbo.stp_UpdateKeyMaxValue', $this.Group.ArcSqlConnection) 
        $sqlCommand.CommandType = [System.Data.CommandType]'StoredProcedure'
        $pProcessStateId = $sqlCommand.Parameters.Add('@ProcessStateId', [int])
        $pProcessStateId.Value = $this.Id

        $this.KeyMaxValue = [int]$sqlCommand.ExecuteScalar()
    }

    UpdateKeyCopyDate()
    {
        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new('dbo.stp_UpdateProcessState', $this.Group.ArcSqlConnection) 
        $sqlCommand.CommandType = [System.Data.CommandType]'StoredProcedure'
        $pProcessStateId = $sqlCommand.Parameters.Add('@ProcessStateId', [int])
        $pProcessStateId.Value = $this.Id
        $pKeyCopyDate = $sqlCommand.Parameters.Add('@KeyCopyDate', [DateTime])
        $pKeyCopyDate.Value = [DateTime]::Now

        $sqlCommand.ExecuteNonQuery()
    }

    UpdateCompleteDate()
    {
        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new('dbo.stp_UpdateProcessState', $this.Group.ArcSqlConnection) 
        $sqlCommand.CommandType = [System.Data.CommandType]'StoredProcedure'
        $pProcessStateId = $sqlCommand.Parameters.Add('@ProcessStateId', [int])
        $pProcessStateId.Value = $this.Id
        $pCompleteDate = $sqlCommand.Parameters.Add('@CompleteDate', [DateTime])
        $pCompleteDate.Value = [DateTime]::Now

        $sqlCommand.ExecuteNonQuery()
    }

    UpdateArchiveState()
    {
        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new('dbo.stp_UpdateProcessState', $this.Group.ArcSqlConnection) 
        $sqlCommand.CommandType = [System.Data.CommandType]'StoredProcedure'
        $pProcessStateId = $sqlCommand.Parameters.Add('@ProcessStateId', [int])
        $pProcessStateId.Value = $this.Id
        $pLastArchivedKey = $sqlCommand.Parameters.Add('@LastArchivedKey', [int])
        $pLastArchivedKey.Value = $this.LastArchivedKey
        $pRowsCopied = $sqlCommand.Parameters.Add('@RowsCopied', [int])
        $pRowsCopied.Value = $this.RowsCopied
        $pLastArchivedDate = $sqlCommand.Parameters.Add('@LastArchivedDate', [DateTime])
        $pLastArchivedDate.Value = [DateTime]::Now

        $sqlCommand.ExecuteNonQuery()
    }

    UpdatePurgeState()
    {
        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new('dbo.stp_UpdateProcessState', $this.Group.ArcSqlConnection) 
        $sqlCommand.CommandType = [System.Data.CommandType]'StoredProcedure'
        $pProcessStateId = $sqlCommand.Parameters.Add('@ProcessStateId', [int])
        $pProcessStateId.Value = $this.Id
        $pLastPurgedKey = $sqlCommand.Parameters.Add('@LastPurgedKey', [int])
        $pLastPurgedKey.Value = $this.LastPurgedKey
        $pRowsPurged = $sqlCommand.Parameters.Add('@RowsPurged', [int])
        $pRowsPurged.Value = $this.RowsPurged
        $pLastPurgedDate = $sqlCommand.Parameters.Add('@LastPurgedDate', [DateTime])
        $pLastPurgedDate.Value = [DateTime]::Now

        $sqlCommand.ExecuteNonQuery()
    }

    Create()
    {
        if ($this.Id -ne 0)
        {
            return
        }

        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new('dbo.stp_InsertProcessState', $this.Group.ArcSqlConnection) 
        $sqlCommand.CommandType = [System.Data.CommandType]'StoredProcedure'
        $pSourceTableId = $sqlCommand.Parameters.Add('@SourceTableId', [int])
        $pSourceTableId.Value = $this.SourceTableId

        $this.Id = [long]$sqlCommand.ExecuteScalar()
    }

    [bool]ArhiveProcessHasRowsForNextBatch()
    {
        return ($this.LastArchivedKey -lt $this.KeyMaxValue)
    }

    [bool]PurgeProcessHasRowsForNextBatch()
    {
        return ($this.LastPurgedKey -lt $this.KeyMaxValue)
    }

    [bool]KeysCopied()
    {
        return ($this.KeyCopyDate -eq [DateTime]::MinValue)
    }

    FixAngGetLastArchivedKey()
    {
        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new('dbo.stp_FixLastArchivedKey', $this.Group.ArcSqlConnection) 
        $sqlCommand.CommandType = [System.Data.CommandType]'StoredProcedure'
        $pProcessStateId = $sqlCommand.Parameters.Add('@ProcessStateId', [int])
        $pProcessStateId.Value = $this.Id

        $this.LastArchivedKey = [long]$sqlCommand.ExecuteScalar()
    }

    SetRowsCopied([int]$count)
    {
        $this.RowsCopiedForBatch = $count
    }
}

class SourceTable
{
    [int]$Id
    [string]$SchemaName
    [string]$TableName
    [int]$DataCopyBatchSize
    [int]$PKCopyBatchSize
    [string]$KeyQuery
    [bool]$Archive
    [bool]$Purge
    [int]$DeleteOrder
    [int]$DelayIntervalInSeconds
    [bool]$AlwaysRunCheck
    [string]$SrcWorkingTableName
    [string]$DstWorkingTableName
    [string]$WorkingTableKeyName
    [string]$WorkingTableFlagName

    [TableGroup]$Group
    [ProcessState]$State
    [System.Collections.ArrayList]$SrcColumns
    [System.Collections.ArrayList]$DstColumns
    [System.Collections.ArrayList]$MissedColumns = [System.Collections.ArrayList]::new()
    [bool]$FKDisabled = $false

    GetState()
    {
        [ProcessState]$this.State = [ProcessState]::new()
        $this.State.SourceTableId = $this.Id
        $this.State.Group = $this.Group
        $this.State.IncompleteProcess = $false

        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new('dbo.stp_GetIncompleteProcessState', $this.Group.ArcSqlConnection) 
        $sqlCommand.CommandType = [System.Data.CommandType]'StoredProcedure'
        $pSourceTableId = $sqlCommand.Parameters.Add('@SourceTableId', [string])
        $pSourceTableId.Value = $this.Id

        [System.Data.SqlClient.SqlDataReader]$sqlReader = $sqlCommand.ExecuteReader()

        if ($sqlReader.Read())
        {
            $this.State.Id = $sqlReader['ProcessStateId']

            if ($sqlReader['KeyCopyDate'] -isnot [DBNull])
            {
                $this.State.KeyCopyDate = $sqlReader['KeyCopyDate']
            }
            if ($sqlReader['KeyMaxValue'] -isnot [DBNull])
            {
                $this.State.KeyMaxValue = $sqlReader['KeyMaxValue']
            }

            if ($sqlReader['LastArchivedKey'] -isnot [DBNull])
            {
                $this.State.LastArchivedKey = $sqlReader['LastArchivedKey']
            }
            if ($sqlReader['LastArchivedDate'] -isnot [DBNull])
            {
                $this.State.LastArchivedDate = $sqlReader['LastArchivedDate']
            }
            if ($sqlReader['RowsCopied'] -isnot [DBNull])
            {
                $this.State.RowsCopied = $sqlReader['RowsCopied']
            }
            if ($sqlReader['LastPurgedKey'] -isnot [DBNull])
            {
                $this.State.LastPurgedKey = $sqlReader['LastPurgedKey']
            }
            if ($sqlReader['LastPurgedDate'] -isnot [DBNull])
            {
                $this.State.LastPurgedDate = $sqlReader['LastPurgedDate']
            }
            if ($sqlReader['RowsPurged'] -isnot [DBNull])
            {
                $this.State.RowsPurged = $sqlReader['RowsPurged']
            }

            $this.State.IncompleteProcess = $true
        }
        $sqlReader.Close()
    }

    [bool]IsTableExistsInSource()
    {
        return [SourceTable]::IsTableExists($this.SchemaName, $this.TableName, $this.Group.SrcSqlConnection)
    }

    [bool]IsTableExistsInDestination()
    {
        return [SourceTable]::IsTableExists($this.SchemaName, $this.TableName, $this.Group.DstSqlConnection)
    }

    static [bool]IsTableExists([string]$schemaName, [string]$tableName, [System.Data.SqlClient.SqlConnection]$sqlConnection)
    {
        [string]$tableExistsQuery = '
select	case when exists
			(
			select	1
			from	INFORMATION_SCHEMA.TABLES
			where TABLE_SCHEMA = ''{0}''
                and TABLE_NAME = ''{1}''
				and TABLE_TYPE = ''BASE TABLE''
			)
			then 1
			else 0
		end v' -f $schemaName, $tableName

        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new($tableExistsQuery, $sqlConnection) 

        return [bool]$sqlCommand.ExecuteScalar()
    }

    GetSourceSourceTableColumns()
    {
        LogMessage([string]'Collecting columns for source table [{0}].[{1}]' -f $this.SchemaName, $this.TableName)
        $this.SrcColumns = [SourceTable]::GetTableColumns($this.SchemaName, $this.TableName, $this.Group.SrcSqlConnection)
        LogMessage([string]'Columns collected for source table [{0}].[{1}]' -f $this.SchemaName, $this.TableName)
    }

    GetDestinationSourceTableColumns()
    {
        LogMessage([string]'Collecting columns for destination table [{0}].[{1}]' -f $this.SchemaName, $this.TableName)
        $this.DstColumns = [SourceTable]::GetTableColumns($this.SchemaName, $this.TableName, $this.Group.DstSqlConnection)
        LogMessage([string]'Columns collected for destination table [{0}].[{1}]' -f $this.SchemaName, $this.TableName)
    }

    static [System.Collections.ArrayList] GetTableColumns([string]$schemaName, [string]$tableName, [System.Data.SqlClient.SqlConnection]$sqlConnection)
    {
        [string]$getTableColumnsQuery = '
    select	co.COLUMN_NAME,
		    co.DATA_TYPE +
			    case co.DATA_TYPE
				    when ''sql_variant'' then ''''
				    when ''text'' then ''''
				    when ''ntext'' then ''''
				    when ''xml'' then ''''
				    when ''decimal'' then ''('' + cast(co.NUMERIC_PRECISION as varchar) + '', '' + cast(co.NUMERIC_SCALE as varchar) + '')''
				    else coalesce(''('' + case when co.CHARACTER_MAXIMUM_LENGTH = -1 then ''max'' else cast(co.CHARACTER_MAXIMUM_LENGTH as varchar) end + '')'', '''') 
			    end DATA_TYPE,
            co.COLLATION_NAME,
		    case when exists
			    (
			    select	1
			    from	INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
					    join INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu
					    on ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME and ccu.TABLE_NAME = tc.TABLE_NAME and ccu.TABLE_SCHEMA = tc.TABLE_SCHEMA
			    where tc.CONSTRAINT_TYPE = ''PRIMARY KEY''
				    and tc.TABLE_SCHEMA = co.TABLE_SCHEMA
				    and tc.TABLE_NAME = co.TABLE_NAME
				    and ccu.COLUMN_NAME = co.COLUMN_NAME
			    )
			    then 1
			    else 0
		    end IsPrimaryKey,
            COLUMNPROPERTY(OBJECT_ID(co.TABLE_SCHEMA + ''.'' + co.TABLE_NAME), co.COLUMN_NAME, ''IsComputed'') IsComputed
    from	INFORMATION_SCHEMA.COLUMNS co
    where co.TABLE_SCHEMA = ''{0}'' and co.TABLE_NAME = ''{1}''' -f $schemaName, $tableName

        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new($getTableColumnsQuery, $sqlConnection) 
        [System.Data.SqlClient.SqlDataReader]$sqlReader = $sqlCommand.ExecuteReader()
        
        $columns = [System.Collections.ArrayList]::new()

        if (-not $sqlReader.HasRows)
        {
            $sqlReader.Close()
            return $columns
        }

        $primaryKeyColumnFound = $false
        while ($sqlReader.Read())
        {
            $column = [Column]::new()

            $column.Name = $sqlReader['COLUMN_NAME']
            $column.DataType = $sqlReader['DATA_TYPE']
            if ($sqlReader['COLLATION_NAME'] -isnot [DBNull])
            {
                $column.Collation = $sqlReader['COLLATION_NAME']
            }
            $column.PrimaryKey = $sqlReader['IsPrimaryKey']
            $column.Computed = $sqlReader['IsComputed']

            if ($column.PrimaryKey)
            {
                $primaryKeyColumnFound = $true
            }

            [void]$columns.Add($column)
        }
        $sqlReader.Close()

        if (-not $primaryKeyColumnFound)
        {
            LogMessage([string]'No primary key column(s) found for specified archive table [{0}].[{1}]' -f $schemaName, $tableName)
        }

        return $columns
    }

    [bool]CompareColumns()
    {
        if ($this.SrcColumns.Count -lt $this.DstColumns.Count)
        {
            LogMessage([string]'The table [{0}].[{1}] schema is not the same on source and destination' -f $this.SchemaName, $this.TableName)
            return $false
        }

        for ($s = 0; $s -lt $this.SrcColumns.Count; $s++)
        {
            [Column]$sourceColumn = $this.SrcColumns[$s]
            $columnFound = $false
        
            for ($d = 0; $d -lt $this.DstColumns.Count; $d++)
            {
                [Column]$destinationColumn = $this.DstColumns[$d]

                if ($destinationColumn.Name -eq $sourceColumn.Name)
                {
                    $columnFound = $true
                    if ($destinationColumn.DataType -ne $sourceColumn.DataType -or $destinationColumn.Collation -ne $sourceColumn.Collation `
                        -or $destinationColumn.PrimaryKey -ne $sourceColumn.PrimaryKey -or $destinationColumn.Computed -ne $sourceColumn.Computed)
                    {
                        LogMessage([string]'The column [{0}] isn''t the same in the source and destination table' -f $sourceColumn.Name)
                        return $false
                    }
                    break
                }
            }

            if (-not $columnFound)
            {
                LogMessage([string]'The column [{0}] wasn''t found in the destination table' -f $sourceColumn.Name)
                $this.MissedColumns.Add($sourceColumn)
            }
        }

        return $true
    }

    AddMissedColumns()
    {
        if ($this.MissedColumns.Count -eq 0)
        {
            return
        }

        $alterTableQuery = 'alter table [{0}].[{1}] add ' -f $this.SchemaName, $this.TableName

        foreach ($column in [Column[]]$this.MissedColumns)
        {
            $addColumn = ''

            if ([string]::IsNullOrEmpty($column.Collation))
            {
                $addColumn = '[{0}] {1} null, ' -f $column.Name, $column.DataType
            }
            else
            {
                $addColumn = '[{0}] {1} collate {2} null, ' -f $column.Name, $column.DataType, $column.Collation
            }
            $alterTableQuery = $alterTableQuery + $addColumn
        }
        $alterTableQuery = $alterTableQuery.SubString(0, $alterTableQuery.Length - 2)

        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new($alterTableQuery, $this.Group.DstSqlConnection) 

        $sqlCommand.ExecuteNonQuery()

        LogMessage([string]'Missed columns added for the [{0}].[{1}] table' -f $this.SchemaName, $this.TableName)
    }

    [bool]CreateDestinationTable()
    {
        $srv = New-Object('Microsoft.SqlServer.Management.Smo.Server') $this.Group.SrcServerName
        $db = $srv.Databases[$this.Group.SrcDatabaseName]

        $scripter = New-Object('Microsoft.SqlServer.Management.Smo.Scripter') $srv

        $scripter.Options.Indexes = $false
        $scripter.Options.ClusteredIndexes = $true
        $scripter.Options.ScriptBatchTerminator = $true
        $scripter.Options.NoCommandTerminator = $false
        $scripter.Options.ToFileOnly = $false
        $scripter.Options.NoFileGroup = $true
        $scripter.Options.DriPrimaryKey = $true
        $scripter.Options.NoCollation = $false

        foreach ($t in $db.Tables)
        {
            if ($t.Schema -eq $this.SchemaName -and $t.Name -eq $this.TableName)
            {
                $script = [string]$scripter.Script($t)
                [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new($script, $this.Group.DstSqlConnection) 

                $sqlCommand.ExecuteNonQuery()

                return $true
            }
        }

        return $false
    }

    DisableEnableFK([bool]$disable)
    {
        if (-not $this.Group.DisableFK)
        {
            return
        }

        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new('dbo.stp_DisableEnableFK', $this.Group.ArcSqlConnection) 
        $sqlCommand.CommandType = [System.Data.CommandType]'StoredProcedure'
        $pProcessStateId = $sqlCommand.Parameters.Add('@ProcessStateId', [int])
        $pProcessStateId.Value = $this.Id
        $pDisable = $sqlCommand.Parameters.Add('@Disable', [bool])
        $pDisable.Value = $disable

        $sqlCommand.ExecuteNonQuery()

        $this.FKDisabled = $disable
    }

    CreateSourceWorkingTable()
    {
        $createSrcWorkingTable = 'if exists(select 1 from INFORMATION_SCHEMA.TABLES where TABLE_NAME = ''{0}'' and TABLE_SCHEMA = ''dbo'' and TABLE_TYPE = ''BASE TABLE'') drop table dbo.[{0}]
    create table dbo.[{0}] ([{1}] int identity(1,1) not null primary key clustered, [{2}] bit not null default 0, ' `
            -f $this.SrcWorkingTableName, $this.WorkingTableKeyName, $this.WorkingTableFlagName

        foreach ($column in [Column[]]$this.SrcColumns)
        {
            if ($column.PrimaryKey)
            {
                if ([string]::IsNullOrEmpty($column.Collation))
                {
                    $createSrcWorkingTable = $createSrcWorkingTable + '[{0}] {1} null, ' -f $column.Name, $column.DataType
                }
                else
                {
                    $createSrcWorkingTable = $createSrcWorkingTable + '[{0}] {1} collate {2} null, ' -f $column.Name, $column.DataType, $column.Collation
                }
            }
        }
        $createSrcWorkingTable = $createSrcWorkingTable.SubString(0, $createSrcWorkingTable.Length - 2) + ')'

        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new($createSrcWorkingTable, $this.Group.ArcSqlConnection) 

        $sqlCommand.ExecuteNonQuery()
    }

    CreateDestinationWorkingTable()
    {
        $createDestinationWorkingTable = 'if exists(select 1 from INFORMATION_SCHEMA.TABLES where TABLE_NAME = ''{0}'' and TABLE_SCHEMA = ''dbo'' and TABLE_TYPE = ''BASE TABLE'') drop table dbo.[{0}]
    create table dbo.[{0}] ([{1}] int identity(1,1) not null primary key clustered, ' `
            -f $this.DstWorkingTableName, $this.WorkingTableKeyName

        foreach ($column in [Column[]]$this.SrcColumns)
        {
                if ([string]::IsNullOrEmpty($column.Collation))
                {
                    $createDestinationWorkingTable = $createDestinationWorkingTable + '[{0}] {1} null, ' -f $column.Name, $column.DataType
                }
                else
                {
                    $createDestinationWorkingTable = $createDestinationWorkingTable + '[{0}] {1} collate {2} null, ' -f $column.Name, $column.DataType, $column.Collation
                }
        }
        $createDestinationWorkingTable = $createDestinationWorkingTable.SubString(0, $createDestinationWorkingTable.Length - 2) + ')'

        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new($createDestinationWorkingTable, $this.Group.ArcSqlConnection) 

        $sqlCommand.ExecuteNonQuery()
    }

    BulkCopySourcePK()
    {
        # source
        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new($this.KeyQuery, $this.Group.SrcSqlConnection) 
        [System.Data.SqlClient.SqlDataReader] $sqlReader = $sqlCommand.ExecuteReader()

        if (-not $sqlReader.HasRows)
        {
            $sqlReader.Close()
            return
        }

        # destination
        [System.Data.SqlClient.SqlBulkCopy]$bulkCopy = [System.Data.SqlClient.SqlBulkCopy]::new($this.Group.ArcSqlConnection.ConnectionString)

        $bulkCopy.DestinationTableName = $this.SrcWorkingTableName
        $bulkCopy.EnableStreaming = $true
        $bulkCopy.BatchSize = $this.PKCopyBatchSize
        foreach ($c in [Column[]]$this.SrcColumns)
        {
            if ($c.PrimaryKey)
            {
                [void]$bulkCopy.ColumnMappings.Add([System.Data.SqlClient.SqlBulkCopyColumnMapping]::new($c.Name, $c.Name))
            }
        }

        $bulkCopy.WriteToServer($sqlReader)
        $bulkCopy.Close()
        $sqlReader.Close()
    }

    BulkCopyDestinationPK()
    {
        # source
        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new($this.KeyQuery, $this.Group.DstSqlConnection) 
        [System.Data.SqlClient.SqlDataReader] $sqlReader = $sqlCommand.ExecuteReader()

        if (-not $sqlReader.HasRows)
        {
            $sqlReader.Close()
            return
        }

        # destination
        [System.Data.SqlClient.SqlBulkCopy]$bulkCopy = [System.Data.SqlClient.SqlBulkCopy]::new($this.Group.ArcSqlConnection.ConnectionString)

        $bulkCopy.DestinationTableName = $this.DstWorkingTableName
        $bulkCopy.EnableStreaming = $true
        $bulkCopy.BatchSize = $this.PKCopyBatchSize
        foreach ($c in [Column[]]$this.SrcColumns)
        {
            if ($c.PrimaryKey)
            {
                [void]$bulkCopy.ColumnMappings.Add([System.Data.SqlClient.SqlBulkCopyColumnMapping]::new($c.Name, $c.Name))
            }
        }

        $bulkCopy.WriteToServer($sqlReader)
        $bulkCopy.Close()
        $sqlReader.Close()
    }

    BulkCopyTable()
    {
        # source
        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new('dbo.stp_GetBulkCopyData', $this.Group.ArcSqlConnection) 
        $sqlCommand.CommandType = [System.Data.CommandType]'StoredProcedure'
        $pProcessStateId = $sqlCommand.Parameters.Add('@ProcessStateId', [int])
        $pProcessStateId.Value = $this.State.Id
        [System.Data.SqlClient.SqlDataReader]$sqlReader = $sqlCommand.ExecuteReader()

        if (-not $sqlReader.HasRows)
        {
            $sqlReader.Close()
            return
        }

        # destination
        [System.Data.SqlClient.SqlBulkCopyOptions] $options = [System.Data.SqlClient.SqlBulkCopyOptions]::KeepIdentity `
            -bor [System.Data.SqlClient.SqlBulkCopyOptions]::KeepNulls
        [System.Data.SqlClient.SqlBulkCopy]$bulkCopy = [System.Data.SqlClient.SqlBulkCopy]::new($this.Group.DstSqlConnection.ConnectionString, $options)

        $bulkCopy.DestinationTableName = '[{0}].[{1}]' -f $this.SchemaName, $this.TableName
        $bulkCopy.EnableStreaming = $true
        $bulkCopy.NotifyAfter = $this.DataCopyBatchSize / 10
        $bulkCopy.Add_SQlRowsCopied( {$table.State.SetRowsCopied($args[1].RowsCopied)} )

        foreach ($c in [Column[]]$this.SrcColumns)
        {
            if (-not $c.Computed)
            {
                [void]$bulkCopy.ColumnMappings.Add([System.Data.SqlClient.SqlBulkCopyColumnMapping]::new($c.Name, $c.Name))
            }
        }

        $bulkCopy.WriteToServer($sqlReader)
        $bulkCopy.Close()
        $sqlReader.Close()
    }

    PurgeData()
    {
        [System.Data.SqlClient.SqlCommand]$sqlCommand = [System.Data.SqlClient.SqlCommand]::new('dbo.stp_PurgeData', $this.Group.ArcSqlConnection) 
        $sqlCommand.CommandType = [System.Data.CommandType]'StoredProcedure'
        $pProcessStateId = $sqlCommand.Parameters.Add('@ProcessStateId', [int])
        $pProcessStateId.Value = $this.State.Id

        $this.State.RowsPurgedForBatch = [int]$sqlCommand.ExecuteScalar()
    }
}

function Get-TimeStamp
{
    
    return "[{0:MM/dd/yy} {0:HH:mm:ss}]" -f (Get-Date)
}

function Get-DelayIntervalInSeconds
{
    [OutputType([int])]
    Param
    (
        [parameter(Mandatory=$true)]
        [string]$DelayInterval
    )

    [datetime]$d1 = [datetime]::ParseExact('01010001 {0}' -f $DelayInterval, 'ddMMyyyy HH:mm:ss', $null)
    [datetime]$d2 = [datetime]::ParseExact('01010001 00:00:00', 'ddMMyyyy HH:mm:ss', $null)

    return $d1.Subtract($d2).TotalSeconds
}

function LogMessage
{
    Param
    (
        [parameter(Mandatory=$true)]
        [string]$message
    )
    Write-Host $(Get-TimeStamp) $message
}

Clear-Host

[string]$ArcConnectionString = 'Data Source=CONTRA;Initial Catalog=arc;Integrated Security=True;'

$groupName = 'G1'

try
{
    [TableGroup]$group = [TableGroup]::new($ArcConnectionString, $groupName)
    if ($group.Id -eq 0)
    {
        LogMessage([string]'Specified table group ({0}) not found' -f $groupName)
        exit 1
    }
    else
    {
        LogMessage([string]'Archive group ({0}) found' -f $groupName)
    }

    [System.Collections.ArrayList]$sourceTables = $group.GetSourceTables()

    if ($sourceTables.Count -eq 0)
    {
        LogMessage([string]'No tables found for specified table group ({0})' -f $groupName)
        exit 1
    }
    else
    {
        LogMessage([string]'{0} archive table(s) found' -f $sourceTables.Count)
    }

    foreach ($table in $sourceTables)
    {
        if (-not ($table.IsTableExistsInSource()))
        {
            LogMessage([string]'Error occurred. The source table [{0}].[{1}] wasn''t found' -f $table.SchemaName, $table.TableName)
            exit 1
        }
        if (-not ($table.IsTableExistsInDestination()))
        {
            LogMessage([string]'The destination table {0}.{1} wasn''t found. Trying to create' -f $table.SchemaName, $table.TableName)
            if (-not $table.CreateDestinationTable())
            {
                LogMessage([string]'Can''t create the destination table [{0}].[{1}]' -f $table.SchemaName, $table.TableName)
                exit 1
            }
            LogMessage([string]'The destination table {0}.{1} created' -f $table.SchemaName, $table.TableName)
        }

        $table.GetSourceSourceTableColumns()
        $table.GetDestinationSourceTableColumns()

        if (-not ($table.CompareColumns()))
        {
            LogMessage([string]'The table {0}.{1} schema isn''t the same in source and destination database' -f $table.SchemaName, $table.TableName)
            exit 1
        }
        else
        {
            $table.AddMissedColumns()
            LogMessage([string]'Schema check passed for the table [{0}].[{1}]' -f $table.SchemaName, $table.TableName)
        }

        $table.GetState()
        $CopyPK = $false

        # is it incomplete process?
        if ($table.State.IncompleteProcess)
        {
            LogMessage([string]'Incomplete process found for the table [{0}].[{1}]' -f $table.SchemaName, $table.TableName)

            # KeyCopyDate has value?
            if ($table.State.KeysCopied())
            {
                $CopyPK = $true
            }
        }
        else
        {
            # Create a new record in ProcessState
            $table.State.Create()
            $CopyPK = $true
        }

        if ($CopyPK)
        {
            # Create a working table
            $table.CreateSourceWorkingTable()
            LogMessage([string]'Working table recreated for [{0}].[{1}]' -f $table.SchemaName, $table.TableName)

            # Populate PK values from source and update KeyCopyDate
            LogMessage([string]'PK copy started for the table [{0}].[{1}]' -f $table.SchemaName, $table.TableName)
            $table.BulkCopySourcePK()
            LogMessage([string]'PK values copied for the table [{0}].[{1}]' -f $table.SchemaName, $table.TableName)

            $table.State.UpdateKeyMaxValue()
            $table.State.UpdateKeyCopyDate()
        }

        if ($table.State.IncompleteProcess -or $table.AlwaysRunCheck)
        {
            $table.CreateDestinationWorkingTable()
            $table.BulkCopyDestinationPK()
            $table.State.FixAngGetLastArchivedKey()
            LogMessage([string]'LastArchivedKey fixed for the table [{0}].[{1}]' -f $table.SchemaName, $table.TableName)
        }
        else
        {
            $table.State.LastArchivedKey = 0
            $table.State.RowsCopied = 0
            $table.State.UpdateArchiveState()
        }

        LogMessage([string]'Data copy started for the table [{0}].[{1}]' -f $table.SchemaName, $table.TableName)
        while ($table.State.ArhiveProcessHasRowsForNextBatch())
        {
            $table.BulkCopyTable()

            $table.State.LastArchivedKey = $table.State.LastArchivedKey + $table.DataCopyBatchSize
            $table.State.RowsCopied = $table.State.RowsCopied + $table.State.RowsCopiedForBatch
            $table.State.UpdateArchiveState()
            Start-Sleep -s $table.DelayIntervalInSeconds
        }
        $table.State.UpdateArchiveState()
        LogMessage([string]'Data copy completed for the table [{0}].[{1}]' -f $table.SchemaName, $table.TableName)
    }
    LogMessage([string]'Archive process completed for the group ({0})' -f $group.Name)

    # For each table in a group according to DeleteOrder
    foreach ($table in $sourceTables | Sort-Object -Property DeleteOrder)
    { 
        if ($table.Purge)
        {
            if (-not $table.State.IncompleteProcess)
            {
                $table.State.LastPurgedKey = 0
                $table.State.RowsPurged = 0
                $table.State.UpdatePurgeState()
            }

            LogMessage([string]'Purge started for the table [{0}].[{1}]' -f $table.SchemaName, $table.TableName)
            while ($table.State.PurgeProcessHasRowsForNextBatch())
            {
                $table.DisableEnableFK($true)
                $table.PurgeData()
                $table.DisableEnableFK($false)

                $table.State.LastPurgedKey = $table.State.LastPurgedKey + $table.DataCopyBatchSize
                $table.State.RowsPurged = $table.State.RowsPurged + $table.State.RowsPurgedForBatch
                $table.State.UpdatePurgeState()
                Start-Sleep -s $table.DelayIntervalInSeconds
            }
            LogMessage([string]'Purge completed for the table [{0}].[{1}]' -f $table.SchemaName, $table.TableName)
        }

        $table.State.UpdateCompleteDate()

    }
    LogMessage([string]'Purge process completed for the group ({0})' -f $group.Name)
}
catch
{
	LogMessage($_.Exception.ToString())
    LogMessage([string]'Error occurred. Please check log')
}
exit 0
