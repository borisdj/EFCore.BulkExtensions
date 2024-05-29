using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq.Expressions;

namespace EFCore.BulkExtensions;

/// <summary>
/// Provides configration for EFCore BulkExtensions
/// 为EFCore BulkExtensions提供配置
/// </summary>
public class BulkConfig
{
    /// <summary>
    ///     Makes sure that entites are inserted to Db as ordered in entitiesList.
    ///     确保实体按照实体列表中的顺序插入Db。
    /// </summary>
    /// <value>
    ///     Default value is <c>true</c>, if table has Identity column (autoincrement) and IDs being 0 in list they will temporarily be changed automatically from 0s into range -N:-1.
    ///     默认是true，如果表具有“主键”列（自动递增）并且实体中的id是0，会自动改变
    /// </value>
    public bool PreserveInsertOrder { get; set; } = true;

    /// <summary>
    ///     When set IDs zero values will be updated to new ones from database (Have function only when PK has Identity)
    ///     当设置ID时，零值将更新为数据库中的新值（只有当PK具有标识时才具有功能）
    /// </summary>
    /// <remarks>
    ///     Useful when BulkInsert is done to multiple related tables, to get PK of table and to set it as FK for second one.
    ///     当对多个相关表执行BulkInsert时，获取表的主键并将其设置为第二个表的FK非常有用。
    /// </remarks>
    public bool SetOutputIdentity { get; set; }

    /// <summary>
    ///    Used only when SetOutputIdentity is set to true, and if this remains True (which is default) all columns are reloaded from Db.
    ///    When changed to false only Identity column is loaded.
    ///    仅当SetOutputIdentity设置为true时使用，如果此值保持为True（默认值），则从Db重新加载所有列，
    ///    当更改为false时，仅加载“标识”列。
    /// </summary>
    /// <remarks>
    ///     Used for efficiency to reduce load back from DB.
    ///     用于提高效率以减少从数据库返回的负载。
    /// </remarks>
    public bool SetOutputNonIdentityColumns { get; set; } = true;

    /// <summary>
    ///    Used only when SetOutputIdentity is set to true, and when changed to True then columns that were no included in Upsert are not loaded.
    ///    仅当SetOutputIdentity设置为true时使用，当更改为True时，不加载Upsert中未包含的列。
    /// </summary>
    public bool LoadOnlyIncludedColumns { get; set; } = false;

    /// <summary>
    ///     Propagated to SqlBulkCopy util object.
    ///     已传播到SqlBulkCopy util对象。
    /// </summary>
    /// <value>
    ///     Defalut value is 2000.
    /// </value>
    public int BatchSize { get; set; } = 2000;

    /// <summary>
    ///     Propagated to SqlBulkCopy util object. When not set will have same value of BatchSize, each batch one notification.
    ///     已传播到SqlBulkCopy util对象。如果未设置，则将具有相同的BatchSize值，每个批次一个通知。
    /// </summary>
    public int? NotifyAfter { get; set; }

    /// <summary>
    ///     Propagated to SqlBulkCopy util object. When not set has SqlBulkCopy default which is 30 seconds and if set to 0 it indicates no limit.
    ///     已传播到SqlBulkCopy util对象。如果未设置，则SqlBulkCopy默认值为30秒，如果设置为0，则表示没有限制。
    /// </summary>
    public int? BulkCopyTimeout { get; set; }

    /// <summary>
    ///     When set to <c>true</c> Temp tables are created as #Temporary. More info: <c>https://www.sqlservertutorial.net/sql-server-basics/sql-server-temporary-tables/</c>
    ///     当设置为true时，会创建临时表
    /// </summary>
    /// <remarks>
    ///     If used then BulkOperation has to be inside Transaction, otherwise destination table gets dropped too early because transaction ends before operation is finished.
    ///     若使用BulkOperation，那个么BulkOperation必须在Transaction内部，否则目标表会过早地被丢弃，因为事务在操作完成之前就结束了。
    /// </remarks>
    public bool UseTempDB { get; set; }

    /// <summary>
    ///     When set to false temp table name will be only 'Temp' without random numbers.
    ///     当设置为false时，临时表名称将仅为不带随机数的“temp”。
    /// </summary>
    /// <value>
    ///     Default value is <c>true</c>.
    /// </value>
    public bool UniqueTableNameTempDb { get; set; } = true;

    /// <summary>
    ///     When set it appends 'OPTION (LOOP JOIN)' for SqlServer, to reduce potential deadlocks on tables that have FKs.
    ///     设置时，它会为SqlServer附加“OPTION（LOOP JOIN）”，以减少具有FK的表上潜在的死锁。
    /// </summary>
    /// <remarks>
    ///     Use this hint as a last resort for experienced devs and db admins.
    ///     将此提示作为经验丰富的开发人员和数据库管理员的最后手段。
    /// </remarks>
    /// <value>
    ///     Default value is <c>false</c>.
    /// </value>
    public bool UseOptionLoopJoin { get; set; } = false;

    /// <summary>
    ///     Enables specifying custom name of table in Db that does not have to be mapped to Entity.
    ///     允许指定Db中不必映射到实体的表的自定义名称。
    /// </summary>
    /// <value>
    ///     Can be set with 'TableName' only or with 'Schema.TableName'.
    ///     仅可以设置tableName或Schema.TableName
    /// </value>
    public string? CustomDestinationTableName { get; set; }

    /// <summary>
    ///     Source data from specified table already in Db, so input list not used and can be empty.
    ///     指定表中的源数据已在Db中，因此未使用输入列表，并且可以为空。
    /// </summary>
    /// <value>
    ///     Can be set with 'TableName' only or with 'Schema.TableName' (Not supported for Sqlite).
    ///     仅可以设置TableName或Schema.tableName （不支持sqlite）
    /// </value>
    public string? CustomSourceTableName { get; set; }

    /// <summary>
    ///     Only if CustomSourceTableName is set and used for specifying Source - Destination column names when they are not the same.
    ///     仅当CustomSourceTableName已设置并用于指定源-目标列名称时（当它们不相同时）。
    /// </summary>
    public Dictionary<string, string>? CustomSourceDestinationMappingColumns { get; set; }

    /// <summary>
    ///     When configured data is loaded from this object instead of entity list which should be empty
    ///     当从该对象而不是应该为空的实体列表加载配置的数据时
    /// </summary>
    public IDataReader? DataReader { get; set; }

    /// <summary>
    ///     Can be used when DataReader is also configured and when set it is propagated to SqlBulkCopy util object, useful for big field like blob, binary column.
    ///     也可以在配置DataReader时使用，并且在设置时将其传播到SqlBulkCopy util对象，这对于大字段（如blob、二进制列）非常有用。
    /// </summary>
    public bool EnableStreaming { get; set; }

    /// <summary>
    ///     Can be set to True if want to have tracking of entities from BulkRead or when SetOutputIdentity is set.
    ///     如果要从BulkRead跟踪实体，或者设置了SetOutputIdentity，则可以设置为True。
    /// </summary>
    public bool TrackingEntities { get; set; }

    /// <summary>
    ///     Sql MERGE Statement contains 'WITH (HOLDLOCK)', otherwise if set to <c>false</c> it is removed.
    ///     Sql MERGE语句包含“WITH（HOLDLOCK）”，否则，如果设置为<c>false</c>，它将被删除。
    /// </summary>
    /// <value>
    ///     Default value is <c>true</c>.
    /// </value>
    public bool WithHoldlock { get; set; } = true;

    /// <summary>
    ///     When set to <c>true</c> the result is return in <c>BulkConfig.StatsInfo { StatsNumberInserted, StatsNumberUpdated}</c>.
    ///     当设置为true时结果返回在BulkConfig.StatsInfo { StatsNumberInserted, StatsNumberUpdated}属性中
    /// </summary>
    /// <remarks>
    ///     If used for pure Insert (with Batching) then SetOutputIdentity should also be configured because Merge have to be used.
    ///     如果用于纯插入（带批处理），则还应配置SetOutputIdentity，因为必须使用Merge。
    /// </remarks>
    public bool CalculateStats { get; set; }

    /// <summary>
    ///     Ignore handling RowVersion column.
    ///     忽略处理RowVersion列。
    /// </summary>
    /// <value>
    ///     Default value is <c>false</c>, if table have any RowVersion column, it will have special handling and needs to be binary.
    ///     若表有任何RowVersion列，它将有特殊的处理，并且需要是二进制的。
    /// </value>
    public bool IgnoreRowVersion { get; set; } = false;

    /// <summary>
    ///     Used as object for returning Stats Info when <c>BulkConfig.CalculateStats = true</c>.
    ///     当BulkConfig.CalculateStats = true时，返回的对象
    /// </summary>
    /// <value>
    ///     Contains info in Properties: <c>StatsNumberInserted, StatsNumberUpdated, StatsNumberDeleted</c>
    ///     包含的属性：插入的记录数，更新的记录数，删除的记录数
    /// </value>
    public StatsInfo? StatsInfo { get; internal set; }

    /// <summary>
    ///     Used as object for returning TimeStamp Info when <c>BulkConfig.DoNotUpdateIfTimeStampChanged = true</c>.
    ///     当BulkConfig.DoNotUpdateIfTimeStampChanged = true时，返回TimeStamp信息
    /// </summary>
    public TimeStampInfo? TimeStampInfo { get; internal set; }

    /// <summary>
    ///     When doing Insert/Update properties to affect can be explicitly selected by adding their names into PropertiesToInclude.
    ///     在执行“插入/更新”操作时，可以通过将要影响的属性的名称添加到“要包含的属性”中来显式选择要影响的特性。
    /// </summary>
    /// <remarks>
    ///     If need to change more then half columns then PropertiesToExclude can be used. Setting both Lists are not allowed.
    ///     如果需要更改超过半列，则可以使用Properties ToExclude。不允许同时设置这两个列表。
    /// </remarks>
    public List<string>? PropertiesToInclude { get; set; }

    /// <summary>
    ///     By adding a column name to this list, will allow it to be inserted and updated but will not update the row if any of the these columns in that row did not change.
    ///     通过向该列表中添加列名，将允许插入和更新该列，但若该行中的任何列未更改，则不会更新该行。
    /// </summary>
    /// <remarks>
    ///     For example, if importing data and want to keep an internal UpdateDate, add all columns except that one, or use PropertiesToExcludeOnCompare.
    ///     例如，如果导入数据并希望保留内部UpdateDate，请添加除该列之外的所有列，或者使用PropertiesToExcludeOnCompare。
    /// </remarks>
    public List<string>? PropertiesToIncludeOnCompare { get; set; }


    /// <summary>
    ///     By adding a column name to this list, will allow it to be inserted and updated but will not update the row if any of the others columns in that row did not change.
    ///     通过将列名添加到此列表中，将允许插入和更新列名，但如果该行中的任何其他列没有更改，则不会更新该行。
    /// </summary>
    /// <remarks>
    ///     For example, if importing data and want to keep an internal UpdateDate, add that columns to the UpdateDate.
    ///     例如，如果导入数据并希望保留内部UpdateDate，请将该列添加到UpdateDate。
    /// </remarks>
    public List<string>? PropertiesToExcludeOnCompare { get; set; }

    /// <summary>
    ///     Ensures that only certain columns with selected properties are Updated. Can differ from PropertiesToInclude that can that be used for Insert config only.
    ///     确保仅更新具有选定属性的某些列。可以与只能用于插入配置的PropertiesToInclude不同。
    /// </summary>
    /// <remarks>
    ///     When need to Insert only new and skip existing ones in Db (Insert_if_not_Exist) then use BulkInsertOrUpdate with this list set to empty: <c>new List<string> { "" }</string></c>
    ///     当需要在Db中只插入新的并跳过现有的时（Insert_if_not_Exist），则使用BulkInsertOrUpdate并将此列表设置为空：<c>新列表<string>｛“”｝</string></c>
    /// </remarks>
    public List<string>? PropertiesToIncludeOnUpdate { get; set; }

    /// <summary>
    ///     When doing Insert/Update one or more properties can be exclude by adding their names into PropertiesToExclude.
    ///     执行“插入/更新”时，可以通过将一个或多个属性的名称添加到“PropertiesToExclude”中来排除这些属性。
    /// </summary>
    /// <remarks>
    ///     If need to change less then half column then PropertiesToInclude can be used. Setting both Lists are not allowed.
    ///     如果需要更改少于半列，则可以使用PropertiesToInclude。不允许同时设置这两个列表。
    /// </remarks>
    public List<string>? PropertiesToExclude { get; set; }


    /// <summary>
    ///     Selected properties are excluded from being updated, can differ from PropertiesToExclude that can be used for Insert config only.
    ///     所选属性被排除在更新之外，可以不同于PropertiesToExclude（仅可用于插入配置的）
    /// </summary>
    public List<string>? PropertiesToExcludeOnUpdate { get; set; }

    /// <summary>
    ///     Used for specifying custom properties, by which we want update to be done.
    ///     用于指定自定义属性，我们希望通过该属性进行更新。
    /// </summary>
    /// <remarks>
    ///     If Identity column exists and is not added in UpdateByProp it will be excluded automatically.
    ///     如果“标识”列存在并且未添加到UpdateByProp中，则会自动将其排除在外。
    /// </remarks>
    public List<string>? UpdateByProperties { get; set; }

    /// <summary>
    ///     Used for specifying a function that returns custom SQL to use for conditional updates on merges.
    ///     用于指定一个函数，该函数返回用于合并条件更新的自定义SQL。
    /// </summary>
    /// <remarks>
    ///     Function receives (existingTablePrefix, insertedTablePrefix) and should return the SQL of the WHERE clause.
    ///     函数接收（existingTablePrefix，insertedTablePrefix），并应返回WHERE子句的SQL。
    ///     The SQLite implementation uses UPSERT functionality added in SQLite 3.24.0 (https://www.sqlite.org/lang_UPSERT.html).
    /// </remarks>
    public Func<string, string, string>? OnConflictUpdateWhereSql { get; set; }

    /// <summary>
    ///     When set to <c>true</c> it will adding (normal) Shadow Property and persist value. It Disables automatic discrimator, so it shoud be set manually.
    ///     当设置为true时，它将添加（正常）阴影属性和持久值。它禁用了自动判别器，所以应该手动设置。
    /// </summary>
    public bool EnableShadowProperties { get; set; }


    /// <summary>
    ///     Returns value for shadow properties, EnableShadowProperties = true
    ///     返回阴影属性的值，EnableShadowProperties=true
    /// </summary>
    public Func<object, string, object?>? ShadowPropertyValue { get; set; }


    /// <summary>
    ///    Shadow columns used for Temporal table. Has defaults elements: 'PeriodStart' and 'PeriodEnd'. Can be changed if temporal columns have custom names.
    ///    用于临时表的阴影列。有默认的元素：PeriodStart，PeriodEnd，如果临时列具有自定义名称，则可以更改
    /// </summary>
    public List<string> TemporalColumns { get; set; } = new List<string> { "PeriodStart", "PeriodEnd" };

    /// <summary>
    ///     When set all entites that have relations with main ones from the list are also merged into theirs tables.
    ///     设置时，与列表中的主要实体有关系的所有实体也会合并到它们的表中。
    /// </summary>
    /// <remarks>
    ///     Essentially enables with one call bulk ops on multiple tables that are connected, like parent-child relationship with FK
    ///     本质上，在连接的多个表上启用一次调用批量操作，如与FK的父子关系
    /// </remarks>
    public bool IncludeGraph { get; set; }

    /// <summary>
    ///     Removes the clause 'EXISTS ... EXCEPT' from Merge statement which then updates even same data, useful when need to always active triggers.
    ///     删除子句'EXISTS。。。EXCEPT来自Merge语句，该语句甚至更新相同的数据，在需要始终激活触发器时非常有用。
    /// </summary>
    public bool OmitClauseExistsExcept { get; set; }

    /// <summary>
    ///     When set to <c>true</c> rows with concurrency conflict, meaning TimeStamp column is changed since read, 
    ///     will not be updated their entities will be loaded into <c>BulkConfig.TimeStampInfo { NumberOfSkippedForUpdate, EntitiesOutput }</c>.
    ///     当设置为true是，在并发行冲突时，这意味着TimeStamp列自读取后发生了更改，
    ///     将不会更新，它们的实体将加载到＜c＞BulkConfig.TimeStampInfo｛NumberOfSkippedForUpdate，EntitiesOutput｝。
    /// </summary>
    /// <remarks>
    ///     After reading skipped from EntitiesOutput, they can either be left skipped, or updated again, or thrown exception or rollback entire Update. (example Tests.EFCoreBulkTestAtypical.TimeStampTest)
    ///     从EntitiesOutput读取跳过后，它们可以被跳过，或者再次更新，或者引发异常或回滚整个更新。（测试示例。EFCoreBulkTest非典型。时间戳测试）
    /// </remarks>
    public bool DoNotUpdateIfTimeStampChanged { get; set; }

    /// <summary>
    ///     Default is zero '0'. When set to larger value it appends: LIMIT 'N', to generated query
    ///     默认值是0，当设置为更大的值时，它会在生成的查询中附加：LIMIT'N'
    /// </summary>
    /// <remarks>
    ///     Used only with PostgreSql.
    ///     仅用于PostgreSql数据库
    /// </remarks>
    public int ApplySubqueryLimit { get; set; } = 0;

    /// <summary>
    ///     Spatial Reference Identifier - for SQL Server with NetTopologySuite. Default value is <c>4326</c>.
    ///     空间引用标识符-适用于带有NetTopologySuite的SQL Server。默认值为4326
    /// </summary>
    /// <remarks>
    ///     More info: <c>https://docs.microsoft.com/en-us/sql/relational-databases/spatial/spatial-reference-identifiers-srids</c>
    /// </remarks>
    public int SRID { get; set; } = 4326;

    /// <summary>
    ///     When type dbtype datetime2 has precision less then default 7, 
    ///     for example 'datetime2(3)' SqlBulkCopy does Floor instead of Round so Rounding done in memory to make sure inserted values are same as with regular SaveChanges.
    ///     当类型dbtype datetime2的精度小于默认值7时，例如‘datetime2(3)’，SqlBulkCopy使用Floor代替Round，
    ///     因此在内存中执行Rounding以确保插入的值与常规SaveChanges相同。
    /// </summary>
    /// <remarks>
    ///     Only for SqlServer.
    ///     仅用于sqlserver数据库
    /// </remarks>
    public bool DateTime2PrecisionForceRound { get; set; }

    /// <summary>
    ///     When using BulkSaveChanges with multiply entries that have FK relationship which is Db generated, this set proper value after reading parent PK from Db.
    ///     IF PK are generated in memory like are some Guid then this can be set to false for better efficiency.
    ///    当使用BulkSaveChanges保存大量由数据库生成外键的实体时，从DB读取父主键后设置正确的值
    ///    如果主键在内存中生成，比如guid，可以设置成false以获得更好的效率
    /// </summary>
    /// <remarks>
    ///     Only used with BulkSaveChanges.
    ///     仅与BulkSaveChanges一起使用。
    /// </remarks>
    public bool OnSaveChangesSetFK { get; set; } = true;

    /// <summary>
    ///     When set to True it ignores GlobalQueryFilters if they exist on the DbSet.
    ///     如果设置为True，则会忽略数据库集中存在的GlobalQueryFilters。
    /// </summary>
    public bool IgnoreGlobalQueryFilters { get; set; }

    /// <summary>
    ///     When set to <c>true</c> result of BulkRead operation will be provided using replace instead of update. Entities list parameter of BulkRead method will be repopulated with obtained data.
    ///     Enables functionality of Contains/IN which will return all entities matching the criteria and only return the first (does not have to be by unique columns).
    ///     当设置为true时，BulkRead操作的结果将使用替换而不是更新来提供。BulkRead方法的实体列表参数将用获得的数据重新填充。
    ///     启用Contains/IN的功能，该功能将返回所有符合条件的实体，并且只返回第一个实体（不必按唯一列）。
    /// </summary>
    public bool ReplaceReadEntities { get; set; }

    /// <summary>
    ///     Enum with [Flags] attribute which enables specifying one or more options.
    ///     具有[Flags]属性的枚举，该属性允许指定一个或多个选项。
    /// </summary>
    /// <value>
    ///     <c>Default, KeepIdentity, CheckConstraints, TableLock, KeepNulls, FireTriggers, UseInternalTransaction</c>
    /// </value>
    public Microsoft.Data.SqlClient.SqlBulkCopyOptions SqlBulkCopyOptions { get; set; } // is superset of System.Data.SqlClient.SqlBulkCopyOptions, gets converted to the desired type

    /// <summary>
    ///     List of column order hints for improving performance.
    ///     为性能提示的实体列顺序提示
    /// </summary>
    public List<SqlBulkCopyColumnOrderHint>? SqlBulkCopyColumnOrderHints { get; set; }

    /// <summary>
    ///     A filter on entities to delete when using BulkInsertOrUpdateOrDelete.
    ///     使用BulkInsertOrUpdateOrDelete时删除实体的过滤器
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
    ///     使用BulkInsertOrUpdateOrDelete时要删除的实体上的筛选器。
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
    /// 用于设置基础数据库连接的函数。
    /// </summary>
    public Func<DbConnection, DbConnection>? UnderlyingConnection { get; set; }

    /// <summary>
    /// A func to set the underlying DB transaction.
    /// 用于设置基础DB事务的函数。
    /// </summary>
    public Func<DbTransaction, DbTransaction>? UnderlyingTransaction { get; set; }

    internal OperationType OperationType { get; set; }

    internal object? SynchronizeFilter { get; private set; }

    internal object? SynchronizeSoftDelete { get; private set; }
}

/// <summary>
/// Class to provide information about how many records have been updated, deleted and inserted.
/// 类，以提供有关已更新、删除和插入的记录数的信息。
/// </summary>
public class StatsInfo
{
    /// <summary>
    /// Indicates the number of inserted records.
    /// 指示插入的记录数。
    /// </summary>
    public int StatsNumberInserted { get; set; }

    /// <summary>
    /// Indicates the number of updated records.
    /// 指示更新的记录数。
    /// </summary>
    public int StatsNumberUpdated { get; set; }

    /// <summary>
    /// Indicates the number of deleted records.
    /// 指示已删除记录的数量。
    /// </summary>
    public int StatsNumberDeleted { get; set; }
}

/// <summary>
/// Provides information about entities.
/// 提供有关实体的信息。
/// </summary>
public class TimeStampInfo
{
    /// <summary>
    /// Indicates the number of entities skipped for an update.
    /// 指示跳过更新的实体数。
    /// </summary>
    public int NumberOfSkippedForUpdate { get; set; }

    /// <summary>
    /// Output the entities.
    /// 输出实体。
    /// </summary>
    public List<object> EntitiesOutput { get; set; } = null!;
}
