using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;

namespace EFCore.BulkExtensions;

/// <summary>
/// Provides configuration for EFCore BulkExtensions
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
    ///    Used only when SetOutputIdentity is set to true, and if this remains True (which is default) all columns are reloaded from Db.
    ///    When changed to false only Identity column is loaded.
    /// </summary>
    /// <remarks>
    ///     Used for efficiency to reduce load back from DB.
    /// </remarks>
    public bool SetOutputNonIdentityColumns { get; set; } = true;

    /// <summary>
    /// Automatically exclude properties marked as Timestamp / RowVersion during Bulk operations.
    /// </summary>
    public bool AutoExcludeTimestamp { get; set; } = true;
    /// <summary>
    ///    Used only when SetOutputIdentity is set to true, and when changed to True then columns that were no included in Upsert are not loaded.
    /// </summary>
    public bool LoadOnlyIncludedColumns { get; set; } = false;

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
    ///     When set to <c>true</c> temp tables are created as #Temporary. More info: <c>https://www.sqlservertutorial.net/sql-server-basics/sql-server-temporary-tables/</c>
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
    ///     When set to <c>true</c> helper(temp) tables are created as #UNLOGGED - only for Postgres. More info: <c>https://www.postgresql.org/docs/current/sql-createtable.html</c>
    /// </summary>
    /// <remarks>
    ///     UNLOGGED is appended only when UseTempDB is False, since Temporary created tables are not logged by default.
    /// </remarks>
    public bool UseUnlogged { get; set; }

    /// <summary>
    ///     When set it appends 'OPTION (LOOP JOIN)' for SqlServer, to reduce potential deadlocks on tables that have FKs.
    /// </summary>
    /// <remarks>
    ///     Use this hint as a last resort for experienced devs and db admins.
    /// </remarks>
    /// <value>
    ///     Default value is <c>false</c>.
    /// </value>
    public bool UseOptionLoopJoin { get; set; } = false;

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
    ///     When configured data is loaded from this object instead of entity list which should be empty
    /// </summary>
    public IDataReader? DataReader { get; set; }

    /// <summary>
    ///     Can be used when DataReader is also configured and when set it is propagated to SqlBulkCopy util object, useful for big field like blob, binary column.
    /// </summary>
    public bool EnableStreaming { get; set; }

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
    ///     When doing Insert/Update properties to affect can be explicitly selected by adding their names into PropertiesToInclude.
    /// </summary>
    /// <remarks>
    ///     If need to change more then half columns then PropertiesToExclude can be used. Setting both Lists are not allowed.
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
    ///     When doing Insert/Update one or more properties can be exclude by adding their names into PropertiesToExclude.
    /// </summary>
    /// <remarks>
    ///     If need to change less then half column then PropertiesToInclude can be used. Setting both Lists are not allowed.
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
    public List<string> TemporalColumns { get; set; } = new List<string> { "PeriodEnd", "PeriodStart" };
    
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
    ///     Default is zero '0'. When set to larger value it appends: LIMIT 'N', to generated query
    /// </summary>
    /// <remarks>
    ///     Used only with PostgreSql.
    /// </remarks>
    public int ApplySubqueryLimit { get; set; } = 0;

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
    ///     When set to <c>true</c> result of BulkRead operation will be provided using replace instead of update. Entities list parameter of BulkRead method will be repopulated with obtained data.
    ///     Enables functionality of Contains/IN which will return all entities matching the criteria and only return the first (does not have to be by unique columns).
    /// </summary>
    public bool ReplaceReadEntities { get; set; }

    /// <summary>
    ///     If used, should be set to valid pure Sql syntax, that would be run after main operation but before deleting temporary tables.
    ///     One practical use case would be to move data from TempOutput table (set UniqueTableNameTempDb to know the name) into a some Log table, optionally using FOR JSON PATH (Test: CustomSqlPostProcessTest).
    /// </summary>
    public string? CustomSqlPostProcess { get; set; }

    /// <summary>
    ///     Enum with [Flags] attribute which enables specifying one or more options.
    /// </summary>
    /// <value>
    ///     <c>Default, KeepIdentity, CheckConstraints, TableLock, KeepNulls, FireTriggers, UseInternalTransaction</c>
    /// </value>
    public SqlBulkCopyOptions SqlBulkCopyOptions { get; set; } // is superset of System.Data.SqlClient.SqlBulkCopyOptions, gets converted to the desired type

    /// <summary>
    ///     List of column order hints for improving performance.
    /// </summary>
    public List<SqlBulkCopyColumnOrderHint>? SqlBulkCopyColumnOrderHints { get; set; }

    /// <summary>
    /// Set MySqlBulkLoaderConflictOption
    /// </summary>
    public ConflictOption ConflictOption { get; set; } = ConflictOption.None;

    /// <summary>
    ///     A filter on entities to delete when using BulkInsertOrUpdateOrDelete.
    /// </summary>
    public void SetSynchronizeFilter<T>(Expression<Func<T, bool>> filter) where T : class
    {
        SynchronizeFilter = filter;
    }
    /// <summary>
    ///     Clears SynchronizeFilter
    /// </summary>
    public void ClearSynchronizeFilter()
    {
        SynchronizeFilter = null;
    }

    /// <summary>
    ///     A filter on entities to delete when using BulkInsertOrUpdateOrDelete.
    /// </summary>
    public void SetSynchronizeSoftDelete<T>(Expression<Func<T, T>> softDelete) where T : class
    {
        SynchronizeSoftDelete = softDelete;
    }
    /// <summary>
    ///     Clear SoftDelete
    /// </summary>
    public void ClearSoftDelete()
    {
        SynchronizeSoftDelete = null;
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

    internal object? SynchronizeSoftDelete { get; private set; }
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

/// <summary>
/// Bitwise flag that specifies one or more options to use with an instance of <see cref="T:Microsoft.Data.SqlClient.SqlBulkCopy"/>.
/// </summary>
[Flags]
public enum SqlBulkCopyOptions
{
    /// <summary>
    /// Use the default values for all options.
    /// </summary>
    Default = 0,
    /// <summary>
    /// Preserve source identity values. When not specified, identity values are assigned by the destination.
    /// </summary>
    KeepIdentity = 1 << 0,
    /// <summary>
    /// Check constraints while data is being inserted. By default, constraints are not checked.
    /// </summary>
    CheckConstraints = 1 << 1,
    /// <summary>
    /// Obtain a bulk update lock for the duration of the bulk copy operation. When not specified, row locks are used.
    /// </summary>
    TableLock = 1 << 2,
    /// <summary>
    /// Preserve null values in the destination table regardless of the settings for default values.
    /// When not specified, null values are replaced by default values where applicable.
    /// </summary>
    KeepNulls = 1 << 3,
    /// <summary>
    /// When specified, cause the server to fire the insert triggers for the rows being inserted into the database.
    /// </summary>
    FireTriggers = 1 << 4,
    /// <summary>
    /// When specified, each batch of the bulk-copy operation will occur within a transaction.
    /// If you indicate this option and also provide a <see cref="T:Microsoft.Data.SqlClient.SqlTransaction" />object to the constructor,
    /// an <see cref="T:System.ArgumentException" /> occurs.
    /// </summary>
    UseInternalTransaction = 1 << 5,
    /// <summary>
    /// When specified, **AllowEncryptedValueModifications** enables bulk copying of encrypted data between tables or databases, without decrypting the data.
    /// </summary>
    AllowEncryptedValueModifications = 1 << 6
}

/// <summary>
/// Defines the sort order for a column in a <see cref="T:Microsoft.Data.SqlClient.SqlBulkCopy" /> instance's destination table,
/// according to the clustered index on the table.
/// </summary>
public class SqlBulkCopyColumnOrderHint
{
    /// <summary>
    /// The name of the destination column within the destination table.
    /// </summary>
    public required string Column { get; set; }

    /// <summary>
    /// The sort order of the corresponding destination column.
    /// </summary>
    public SortOrder SortOrder { get; set; } = SortOrder.Unspecified;
}

/// <summary>
/// Specifies how rows of data are sorted.
/// </summary>
public enum SortOrder
{
    /// <summary>
    /// The default. No sort order is specified.
    /// </summary>
    Unspecified = -1,
    /// <summary>
    /// Rows are sorted in ascending order.
    /// </summary>
    Ascending = 0,
    /// <summary>
    /// Rows are sorted in descending order.
    /// </summary>
    Descending = 1
}

/// <summary>
/// Conflict option for Bulk Insert
/// </summary>
public enum ConflictOption
{
    /// <summary>
    /// Treat conflicts as errors
    /// </summary>
    None,

    /// <summary>
    /// Replace conflicting rows with new rows
    /// </summary>
    Replace,

    /// <summary>
    /// Ignore conflicting rows and keep old rows
    /// </summary>
    Ignore
}

