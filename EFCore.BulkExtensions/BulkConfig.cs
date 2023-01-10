using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq.Expressions;

namespace EFCore.BulkExtensions;

/// <summary>
/// Provides configration for EFCore BulkExtensions
/// </summary>
public class BulkConfig
{
    /// <summary>
    ///     Makes sure that entites are inserted to Db as ordered in entitiesList.
    /// </summary>
    /// <value>
    ///     Default value is <c>true</c>, if table has Identity column (autoincrement) and IDs being 0 in list they will temporarily be changed automatically from 0s into range -N:-1.
    /// </value>
    public bool PreserveInsertOrder { get; set; } = true;

    /// <summary>
    ///     When set IDs zero values will be updated to new ones from database (Have function only when PK has Identity)
    /// </summary>
    /// <remarks>
    ///     Useful when BulkInsert is done to multiple related tables, to get PK of table and to set it as FK for second one.
    /// </remarks>
    public bool SetOutputIdentity { get; set; }

    /// <summary>
    ///     Propagated to SqlBulkCopy util object.
    /// </summary>
    /// <value>
    ///     Defalut value is 2000.
    /// </value>
    public int BatchSize { get; set; } = 2000;

    /// <summary>
    ///     Propagated to SqlBulkCopy util object. When not set will have same value of BatchSize, each batch one notification.
    /// </summary>
    public int? NotifyAfter { get; set; }

    /// <summary>
    ///     Propagated to SqlBulkCopy util object. When not set has SqlBulkCopy default which is 30 seconds and if set to 0 it indicates no limit.
    /// </summary>
    public int? BulkCopyTimeout { get; set; }

    /// <summary>
    ///     Propagated to SqlBulkCopy util object.
    /// </summary>
    public bool EnableStreaming { get; set; }

    /// <summary>
    ///     When set to <c>true</c> Temp tables are created as #Temporary. More info: <c>https://www.sqlservertutorial.net/sql-server-basics/sql-server-temporary-tables/</c>
    /// </summary>
    /// <remarks>
    ///     If used then BulkOperation has to be inside Transaction, otherwise destination table gets dropped too early because transaction ends before operation is finished.
    /// </remarks>
    public bool UseTempDB { get; set; }

    /// <summary>
    ///     When set to false temp table name will be only 'Temp' without random numbers.
    /// </summary>
    /// <value>
    ///     Default value is <c>true</c>.
    /// </value>
    public bool UniqueTableNameTempDb { get; set; } = true;

    /// <summary>
    ///     Enables specifying custom name of table in Db that does not have to be mapped to Entity.
    /// </summary>
    /// <value>
    ///     Can be set with 'TableName' only or with 'Schema.TableName'.
    /// </value>
    public string? CustomDestinationTableName { get; set; }

    /// <summary>
    ///     Source data from specified table already in Db, so input list not used and can be empty.
    /// </summary>
    /// <value>
    ///     Can be set with 'TableName' only or with 'Schema.TableName' (Not supported for Sqlite).
    /// </value>
    public string? CustomSourceTableName { get; set; }

    /// <summary>
    ///     Only if CustomSourceTableName is set and used for specifying Source - Destination column names when they are not the same.
    /// </summary>
    public Dictionary<string, string>? CustomSourceDestinationMappingColumns { get; set; }

    /// <summary>
    ///     Can be set to True if want to have tracking of entities from BulkRead or when SetOutputIdentity is set.
    /// </summary>
    public bool TrackingEntities { get; set; }

    /// <summary>
    ///     Sql MERGE Statement contains 'WITH (HOLDLOCK)', otherwise if set to <c>false</c> it is removed.
    /// </summary>
    /// <value>
    ///     Default value is <c>true</c>.
    /// </value>
    public bool WithHoldlock { get; set; } = true;

    /// <summary>
    ///     When set to <c>true</c> the result is return in <c>BulkConfig.StatsInfo { StatsNumberInserted, StatsNumberUpdated}</c>.
    /// </summary>
    /// <remarks>
    ///     If used for pure Insert (with Batching) then SetOutputIdentity should also be configured because Merge have to be used.
    /// </remarks>
    public bool CalculateStats { get; set; }

    /// <summary>
    ///     Ignore handling RowVersion column.
    /// </summary>
    /// <value>
    ///     Default value is <c>false</c>, if table have any RowVersion column, it will have special handling and needs to be binary.
    /// </value>
    public bool IgnoreRowVersion { get; set; } = false;

    /// <summary>
    ///     Used as object for returning Stats Info when <c>BulkConfig.CalculateStats = true</c>.
    /// </summary>
    /// <value>
    ///     Contains info in Properties: <c>StatsNumberInserted, StatsNumberUpdated, StatsNumberDeleted</c>
    /// </value>
    public StatsInfo? StatsInfo { get; internal set; }

    /// <summary>
    ///     Used as object for returning TimeStamp Info when <c>BulkConfig.DoNotUpdateIfTimeStampChanged = true</c>.
    /// </summary>
    public TimeStampInfo? TimeStampInfo { get; internal set; }

    /// <summary>
    ///     When doing Insert/Update one or more properties can be exclude by adding their names into PropertiesToExclude.
    /// </summary>
    /// <remarks>
    ///     If need to change less then half column then PropertiesToInclude can be used. Setting both Lists are not allowed.
    /// </remarks>
    public List<string>? PropertiesToInclude { get; set; }

    /// <summary>
    ///     By adding a column name to this list, will allow it to be inserted and updated but will not update the row if any of the these columns in that row did not change.
    /// </summary>
    /// <remarks>
    ///     For example, if importing data and want to keep an internal UpdateDate, add all columns except that one, or use PropertiesToExcludeOnCompare.
    /// </remarks>
    public List<string>? PropertiesToIncludeOnCompare { get; set; }

    /// <summary>
    ///     Ensures that only certain columns with selected properties are Updated. Can differ from PropertiesToInclude that can that be used for Insert config only.
    /// </summary>
    /// <remarks>
    ///     When need to Insert only new and skip existing ones in Db (Insert_if_not_Exist) then use BulkInsertOrUpdate with this list set to empty: <c>new List<string> { "" }</string></c>
    /// </remarks>
    public List<string>? PropertiesToIncludeOnUpdate { get; set; }

    /// <summary>
    ///     When doing Insert/Update properties to affect can be explicitly selected by adding their names into PropertiesToInclude.
    /// </summary>
    /// <remarks>
    ///     If need to change more then half columns then PropertiesToExclude can be used. Setting both Lists are not allowed.
    /// </remarks>
    public List<string>? PropertiesToExclude { get; set; }

    /// <summary>
    ///     By adding a column name to this list, will allow it to be inserted and updated but will not update the row if any of the others columns in that row did not change.
    /// </summary>
    /// <remarks>
    ///     For example, if importing data and want to keep an internal UpdateDate, add that columns to the UpdateDate.
    /// </remarks>
    public List<string>? PropertiesToExcludeOnCompare { get; set; }

    /// <summary>
    ///     Selected properties are excluded from being updated, can differ from PropertiesToExclude that can be used for Insert config only.
    /// </summary>
    public List<string>? PropertiesToExcludeOnUpdate { get; set; }

    /// <summary>
    ///     Used for specifying custom properties, by which we want update to be done.
    /// </summary>
    /// <remarks>
    ///     If Identity column exists and is not added in UpdateByProp it will be excluded automatically.
    /// </remarks>
    public List<string>? UpdateByProperties { get; set; }
    
    /// <summary>
    ///     Used for specifying a function that returns custom SQL to use for conditional updates on merges.
    /// </summary>
    /// <remarks>
    ///     Function receives (existingTablePrefix, insertedTablePrefix) and should return the SQL of the WHERE clause.
    ///     The SQLite implementation uses UPSERT functionality added in SQLite 3.24.0 (https://www.sqlite.org/lang_UPSERT.html).
    /// </remarks>
    public Func<string, string, string>? OnConflictUpdateWhereSql { get; set; }

    /// <summary>
    ///     When set to <c>true</c> it will adding (normal) Shadow Property and persist value. It Disables automatic discrimator, so it shoud be set manually.
    /// </summary>
    public bool EnableShadowProperties { get; set; }


    /// <summary>
    ///     Returns value for shadow properties, EnableShadowProperties = true
    /// </summary>
    public Func<object, string, object?>? ShadowPropertyValue { get; set; } 


    /// <summary>
    ///    Shadow columns used for Temporal table. Has defaults elements: 'PeriodStart' and 'PeriodEnd'. Can be changed if temporal columns have custom names.
    /// </summary>
    public List<string> TemporalColumns { get; set; } = new List<string> { "PeriodStart", "PeriodEnd" };
    
    /// <summary>
    ///     When set all entites that have relations with main ones from the list are also merged into theirs tables.
    /// </summary>
    /// <remarks>
    ///     Essentially enables with one call bulk ops on multiple tables that are connected, like parent-child relationship with FK
    /// </remarks>
    public bool IncludeGraph { get; set; }

    /// <summary>
    ///     Removes the clause 'EXISTS ... EXCEPT' from Merge statement which then updates even same data, useful when need to always active triggers.
    /// </summary>
    public bool OmitClauseExistsExcept { get; set; }

    /// <summary>
    ///     When set to <c>true</c> rows with concurrency conflict, meaning TimeStamp column is changed since read, will not be updated their entities will be loaded into <c>BulkConfig.TimeStampInfo { NumberOfSkippedForUpdate, EntitiesOutput }</c>.
    /// </summary>
    /// <remarks>
    ///     After reading skipped from EntitiesOutput, they can either be left skipped, or updated again, or thrown exception or rollback entire Update. (example Tests.EFCoreBulkTestAtypical.TimeStampTest)
    /// </remarks>
    public bool DoNotUpdateIfTimeStampChanged { get; set; }

    /// <summary>
    ///     Spatial Reference Identifier - for SQL Server with NetTopologySuite. Default value is <c>4326</c>.
    /// </summary>
    /// <remarks>
    ///     More info: <c>https://docs.microsoft.com/en-us/sql/relational-databases/spatial/spatial-reference-identifiers-srids</c>
    /// </remarks>
    public int SRID { get; set; } = 4326;

    /// <summary>
    ///     When type dbtype datetime2 has precision less then default 7, for example 'datetime2(3)' SqlBulkCopy does Floor instead of Round so Rounding done in memory to make sure inserted values are same as with regular SaveChanges.
    /// </summary>
    /// <remarks>
    ///     Only for SqlServer.
    /// </remarks>
    public bool DateTime2PrecisionForceRound { get; set; }

    /// <summary>
    ///     When using BulkSaveChanges with multiply entries that have FK relationship which is Db generated, this set proper value after reading parent PK from Db.
    ///     IF PK are generated in memory like are some Guid then this can be set to false for better efficiency.
    /// </summary>
    /// <remarks>
    ///     Only used with BulkSaveChanges.
    /// </remarks>
    public bool OnSaveChangesSetFK { get; set; } = true;

    /// <summary>
    ///     When set to True it ignores GlobalQueryFilters if they exist on the DbSet.
    /// </summary>
    public bool IgnoreGlobalQueryFilters { get; set; }

    /// <summary>
    ///     Enum with [Flags] attribute which enables specifying one or more options.
    /// </summary>
    /// <value>
    ///     <c>Default, KeepIdentity, CheckConstraints, TableLock, KeepNulls, FireTriggers, UseInternalTransaction</c>
    /// </value>
    public Microsoft.Data.SqlClient.SqlBulkCopyOptions SqlBulkCopyOptions { get; set; } // is superset of System.Data.SqlClient.SqlBulkCopyOptions, gets converted to the desired type

    /// <summary>
    ///     List of column order hints for improving performance.
    /// </summary>
    public List<SqlBulkCopyColumnOrderHint>? SqlBulkCopyColumnOrderHints { get; set; }

    /// <summary>
    ///     A filter on entities to delete when using BulkInsertOrUpdateOrDelete.
    /// </summary>
    public void SetSynchronizeFilter<T>(Expression<Func<T, bool>> filter) where T : class
    {
        SynchronizeFilter = filter;
    }

    /// <summary>
    /// A func to set the underlying DB connection.
    /// </summary>
    public Func<DbConnection, DbConnection>? UnderlyingConnection { get; set; }

    /// <summary>
    /// A func to set the underlying DB transaction.
    /// </summary>
    public Func<DbTransaction, DbTransaction>? UnderlyingTransaction { get; set; }

    internal OperationType OperationType { get; set; }

    internal object? SynchronizeFilter { get; private set; }
}

/// <summary>
/// Class to provide information about how many records have been updated, deleted and inserted.
/// </summary>
public class StatsInfo
{
    /// <summary>
    /// Indicates the number of inserted records.
    /// </summary>
    public int StatsNumberInserted { get; set; }

    /// <summary>
    /// Indicates the number of updated records.
    /// </summary>
    public int StatsNumberUpdated { get; set; }

    /// <summary>
    /// Indicates the number of deleted records.
    /// </summary>
    public int StatsNumberDeleted { get; set; }
}

/// <summary>
/// Provides information about entities.
/// </summary>
public class TimeStampInfo
{
    /// <summary>
    /// Indicates the number of entities skipped for an update.
    /// </summary>
    public int NumberOfSkippedForUpdate { get; set; }

    /// <summary>
    /// Output the entities.
    /// </summary>
    public List<object> EntitiesOutput { get; set; } = null!;
}
