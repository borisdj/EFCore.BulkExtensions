using GBS.Data.GBasedbt;
using Microsoft.EntityFrameworkCore;
using NetTopologySuite.Geometries;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SqlAdapters.GBase;
/// <inheritdoc/>
public class GBaseAdapter : ISqlOperationsAdapter
{
    private GBaseQueryBuilder ProviderSqlQueryBuilder => new GBaseQueryBuilder();

    /// <inheritdoc/>
    #region Methods
    // Insert
    public void Insert<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress)
    {
        InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task InsertAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken)
    {
        await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public static async Task InsertAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken)
    {
        var dbContext = context.DbContext;
        GbsConnection? connection = (GbsConnection?)context.DbConnection;
        if (connection == null)
        {
            connection = isAsync ? await OpenAndGetGBaseConnectionAsync(dbContext, cancellationToken).ConfigureAwait(false)
                                 : OpenAndGetGBaseConnection(dbContext);
        }

        bool doExplicitCommit = false;
        GbsTransaction? transaction = null;
        try
        {
            if (dbContext.Database.CurrentTransaction == null)
            {
                doExplicitCommit = true;
            }

            transaction = (GbsTransaction?)context.DbTransaction;
            if (transaction == null || (transaction != null && transaction.Connection == null))
            {
                var dbTransaction = doExplicitCommit ? connection.BeginTransaction()
                                                     : dbContext.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);

                transaction = (GbsTransaction?)dbTransaction;
            }
            else
            {
                doExplicitCommit = false;
            }

            bool insertOneByOne = false;
            var command = GetGBaseCommand(context, type, entities, tableInfo, connection, transaction);
            // TODO: use OneByOne mode to make it strong.
            //foreach (GbsParameter param in command.Parameters)
            //{
            //      if (param.GbsType == GbsType.NVarChar ||
            //        param.GbsType == GbsType.Guid ||
            //        param.GbsType == GbsType.Byte ||
            //        param.GbsType == GbsType.Blob ||
            //        param.GbsType == GbsType.Text ||
            //        param.GbsType == GbsType.Date ||
            //        param.GbsType == GbsType.Clob)
            //    {
            //        insertOneByOne = true;
            //        break;
            //    }
            //}
            insertOneByOne = true;
            if (insertOneByOne)
            {
                // insert one by one.
                command.Prepare();

                int rowsCopied = 0;
                foreach (var item in entities)
                {
                    LoadGBaseValues(tableInfo, item, command, dbContext, OperationType.Insert);

                    if (isAsync)
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        command.ExecuteNonQuery();
                    }

                    ProgressHelper.SetProgress(ref rowsCopied, entities.Count(), tableInfo.BulkConfig, progress);
                }
            }
            else
            {
                // use insert cursor
                type = tableInfo.HasAbstractList ? entities.ElementAt(0)?.GetType()! : type;
                DataTable dataTable = InnerGetDataTable(context, ref type, entities, tableInfo);

                GbsDataAdapter adapter = new GbsDataAdapter();
                adapter.InsertCommand = command;
                adapter.UpdateBatchSize = dataTable.Rows.Count;
                adapter.Update(dataTable);
            }

            if (doExplicitCommit)
            {
                transaction?.Commit();
            }
        }
        catch (Exception)
        {
            if (doExplicitCommit)
            {
                transaction?.Rollback();
            }
            throw;
        }
        finally
        {
            if (doExplicitCommit)
            {
                if (isAsync)
                {
                    if (transaction is not null)
                    {
                        await transaction.DisposeAsync().ConfigureAwait(false);
                    }
                    await dbContext.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
                else
                {
                    transaction?.Dispose();
                    dbContext.Database.CloseConnection();
                }
            }
        }
    }

    // Merge
    /// <inheritdoc/>
    public void Merge<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress) where T : class
    {
        MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task MergeAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await MergeAsync(context, type, entities, tableInfo, operationType, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }
    
    /// <inheritdoc/>
    protected static async Task MergeAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        var dbContext = context.DbContext;
        GbsConnection connection = isAsync ? await OpenAndGetGBaseConnectionAsync(dbContext, cancellationToken).ConfigureAwait(false)
                                                    : OpenAndGetGBaseConnection(dbContext);
        bool doExplicitCommit = false;
        bool tempTableCreated = false;
        GbsTransaction? transaction = null;

        // cmd is created when operation type is InsertOrUpdate, Update and Delete.
        try
        {
            if (dbContext.Database.CurrentTransaction == null)
            {
                doExplicitCommit = true;
            }
            var dbTransaction = doExplicitCommit ? connection.BeginTransaction()
                                                 : dbContext.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);
            transaction = (GbsTransaction?)dbTransaction;

            if (operationType == OperationType.InsertOrUpdate ||
                operationType == OperationType.Update ||
                operationType == OperationType.Delete)
            {
                // create temp table based on tableInfo.TableName.
                using (GbsCommand cmd = connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = GBaseQueryBuilder.CreateTableCopy(tableInfo.TableName, tableInfo.TempTableName);
                    if (isAsync)
                    {
                        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
                tempTableCreated = true;

                // insert data into temp table.
                tableInfo.InsertToTempTable = true;
            }

            tableInfo.BulkConfig.OperationType = OperationType.Insert;
            context.DbConnection = connection;
            context.DbTransaction = transaction;

            // insert into temp table.
            if (isAsync)
            {
                await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, cancellationToken).GetAwaiter().GetResult();
            }

            //if (doExplicitCommit)
            //{
                //transaction?.Commit();
            //}

            //var command = GetGBaseCommand(context, type, entities, tableInfo, connection, transaction);
            //command.Prepare();

            //type = tableInfo.HasAbstractList ? entities.ElementAt(0).GetType() : type;
            //int rowsCopied = 0;
            //foreach (var item in entities)
            //{
            //    LoadGBaseValues(tableInfo, item, command, context, operationType);

            //    if (isAsync)
            //    {
            //        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            //    }
            //    else
            //    {
            //        command.ExecuteNonQuery();
            //    }

            //    ProgressHelper.SetProgress(ref rowsCopied, entities.Count(), tableInfo.BulkConfig, progress);
            //}

            if (tableInfo.BulkConfig.SetOutputIdentity && tableInfo.IdentityColumnName != null)
            {
                if (operationType == OperationType.Insert)
                {
                    //var command = GetGBaseCommand(context, type, entities, tableInfo, connection, transaction);
                    using (var command = connection.CreateCommand())
                    {
                        command.Transaction = transaction;
                        var keyProperty = tableInfo.ColumnToPropertyDictionary.FirstOrDefault(o => o.Key == tableInfo.IdentityColumnName).Value;
                        command.CommandText = GBaseQueryBuilder.SelectLastInsertRowId(keyProperty);

                        object? lastRowIdScalar = isAsync ? await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false)
                                                                : command.ExecuteScalar();
                        SetIdentityForOutput(entities, tableInfo, lastRowIdScalar);
                    }
                }
            }

            if (operationType == OperationType.InsertOrUpdate ||
                operationType == OperationType.Update ||
                operationType == OperationType.Delete)
            {
                using (GbsCommand cmd = connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    if (operationType == OperationType.Delete)
                    {
                        // delete data base on temp table
                        cmd.CommandText = GBaseQueryBuilder.DeleteFromTempTable(tableInfo);
                    }
                    else
                    {
                        // Insert or InsertOrUpdate, marge data from temp table to tableInfo.TableName.
                        cmd.CommandText = GBaseQueryBuilder.MergeIntoTable(tableInfo);
                    }
                    if (isAsync)
                    {
                        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }

            if (tableInfo.BulkConfig.CustomSqlPostProcess != null)
            {
                using (var command = connection.CreateCommand())
                {
                    command.Transaction = transaction;
                    command.CommandText = tableInfo.BulkConfig.CustomSqlPostProcess;
                    if (isAsync)
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }

            if (doExplicitCommit)
            {
                transaction?.Commit();
            }
        }
        catch (Exception)
        {
            if (doExplicitCommit)
            {
                transaction?.Rollback();
            }
            throw;
        }
        finally
        {
            // drop table table.
            if (tempTableCreated)
            {
                using (var cmd = connection.CreateCommand())
                {
                    cmd.Transaction = transaction;
                    cmd.CommandText = GBaseQueryBuilder.DropTable(tableInfo.TempTableName);
                    if (isAsync)
                    {
                        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        cmd.ExecuteNonQuery();
                    }
                }
            }

            if (isAsync)
            {
                if (doExplicitCommit)
                {
                    if (transaction is not null)
                    {
                        await transaction.DisposeAsync().ConfigureAwait(false);
                    }
                }
                await dbContext.Database.CloseConnectionAsync().ConfigureAwait(false);
            }
            else
            {
                if (doExplicitCommit)
                {
                    transaction?.Dispose();
                }
                dbContext.Database.CloseConnection();
            }
            context.DbConnection = null;
            context.DbTransaction = null;
        }
    }

    // Read
    /// <inheritdoc/>
    public void Read<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress) where T : class
    {
        ReadAsync(context, type, entities, tableInfo, progress, isAsync: false, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <inheritdoc/>
    public async Task ReadAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, CancellationToken cancellationToken) where T : class
    {
        await ReadAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
    }
    
    /// <inheritdoc/>
    protected static async Task ReadAsync<T>(BulkContext context, Type type, IEnumerable<T> entities, TableInfo tableInfo, Action<decimal>? progress, bool isAsync, CancellationToken cancellationToken) where T : class
    {
        var dbContext = context.DbContext;
        GbsConnection connection = isAsync ? await OpenAndGetGBaseConnectionAsync(dbContext, cancellationToken).ConfigureAwait(false)
                                                    : OpenAndGetGBaseConnection(dbContext);
        bool doExplicitCommit = false;
        bool tempTableCreated = false;
        GbsTransaction? transaction = null;
        try
        {
            if (dbContext.Database.CurrentTransaction == null)
            {
                doExplicitCommit = true;
            }

            transaction = doExplicitCommit ? connection.BeginTransaction()
                                           : (GbsTransaction?)dbContext.Database.CurrentTransaction?.GetUnderlyingTransaction(tableInfo.BulkConfig);
            using (GbsCommand command = connection.CreateCommand())
            {
                command.Transaction = transaction;

                // Create a temp table.
                command.CommandText = GBaseQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName);
                if (isAsync)
                {
                    await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                }
                else
                {
                    command.ExecuteNonQuery();
                }
            }
            tempTableCreated = true;

            tableInfo.BulkConfig.OperationType = OperationType.Insert;
            tableInfo.InsertToTempTable = true;
            context.DbConnection = connection;
            context.DbTransaction = transaction;

            // insert into temp table.
            if (isAsync)
            {
                await InsertAsync(context, type, entities, tableInfo, progress, isAsync: true, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                InsertAsync(context, type, entities, tableInfo, progress, isAsync: false, cancellationToken).GetAwaiter().GetResult();
            }

            if (doExplicitCommit)
            {
                transaction?.Commit();
            }

            // jonn table and temp table.
            // this might not be an good way, just follow other database.
            List<T> existingEntities;
            var sqlSelectJoinTable = SqlQueryBuilder.SelectJoinTable(tableInfo).Replace("[", "").Replace("]", "");
            Expression<Func<DbContext, IQueryable<T>>> expression = tableInfo.GetQueryExpression<T>(sqlSelectJoinTable, false);
            var compiled = EF.CompileQuery(expression); // instead using Compiled queries
            existingEntities = compiled(dbContext).ToList();

            // update
            if (tableInfo.BulkConfig.ReplaceReadEntities)
            {
                tableInfo.ReplaceReadEntities(entities, existingEntities);
            }
            else
            {
                tableInfo.UpdateReadEntities(entities, existingEntities, dbContext);
            }
        }
        catch (Exception)
        {
            if (doExplicitCommit)
            {
                transaction?.Rollback();
            }
            throw;
        }
        finally
        {
            if (tempTableCreated)
            {
                using (GbsCommand command = connection.CreateCommand())
                {
                    // drop temp table.
                    command.Transaction = transaction;
                    command.CommandText = GBaseQueryBuilder.DropTable(tableInfo.FullTempTableName);
                    if (isAsync)
                    {
                        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }

            if (doExplicitCommit)
            {
                if (isAsync)
                {
                    if (transaction is not null)
                    {
                        await transaction.DisposeAsync().ConfigureAwait(false);
                    }

                    await dbContext.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
                else
                {
                    transaction?.Dispose();
                    dbContext.Database.CloseConnection();
                }
                context.DbConnection = null;
                context.DbTransaction = null;
            }
        }
    }

    /// <inheritdoc/>
    public void Truncate(BulkContext context, TableInfo tableInfo)
    {
        var sql = new GBaseQueryBuilder().TruncateTable(tableInfo.FullTableName);
        context.DbContext.Database.ExecuteSqlRaw(sql);
    }

    /// <inheritdoc/>
    public async Task TruncateAsync(BulkContext context, TableInfo tableInfo, CancellationToken cancellationToken)
    {
        var sql = new GBaseQueryBuilder().TruncateTable(tableInfo.FullTableName);
        await context.DbContext.Database.ExecuteSqlRawAsync(sql, cancellationToken).ConfigureAwait(false);
    }
    #endregion

    internal static void GetParameterInfo(TableInfo tableInfo, string propertyName, out GbsType gbsType, out int column_size, out string? column_name)
    {
        string? column_full_type = string.Empty;
        string column_type = string.Empty;

        bool rc = tableInfo.PropertyColumnNamesDict.TryGetValue(propertyName, out column_name);
        if (rc == false)
        {
            throw new ArgumentException($"Unable to get column name based on propertyname: {propertyName}");
        }
        rc = tableInfo.ColumnNamesTypesDict.TryGetValue(column_name!, out column_full_type);
        if (rc == false)
        {
            throw new ArgumentException($"Unable to get column full type based on column name: {column_name}");
        }

        column_type = column_full_type!;
        column_size = 0;
        var m = Regex.Match(column_full_type!, @"^(?<type>\w+)\s*\(\s*(?<len>\d+)\s*,\s*(?<scale>\d+)\s*\)$");
        if (m.Success)
        {
            column_type = m.Groups["type"].Value;                    // data type of a column, such as varchar
            column_size = Convert.ToInt32(m.Groups["len"].Value);    // the size of a clumn , such as "255"
                                                                     // scale is ignored.
        }
        else
        {
            m = Regex.Match(column_full_type!, @"^(\w+)\s*\(\s*(\d+)\s*\)$");
            if (m.Success)
            {
                column_type = m.Groups[1].Value;                    // data type of a column, such as varchar
                column_size = Convert.ToInt32(m.Groups[2].Value);   // the size of a clumn , such as "255"
            }
        }

        gbsType = GbsType.Char;
        switch (column_type.ToLower())
        {
            case "integer":
            case "int":
                gbsType = GbsType.Integer;
                break;
            case "nchar":
                gbsType = GbsType.NChar;
                break;
            case "varchar":
                gbsType = GbsType.VarChar;
                break;
            case "nvarchar":
                gbsType = GbsType.NVarChar;
                break;
            case "char":
                gbsType = GbsType.Char;
                break;
            case "lvarchar":
                gbsType = GbsType.LVarChar;
                break;
            case "date":
                gbsType = GbsType.Date;
                break;
            case "decimal":
                gbsType = GbsType.Decimal;
                break;
            case "money":
                gbsType = GbsType.Money;
                break;
            case "bigint":
                gbsType = GbsType.BigInt;
                break;
            case "boolean":
                gbsType = GbsType.Boolean;
                break;
            case "int8":
                gbsType = GbsType.Int8;
                break;
            case "smallint":
                gbsType = GbsType.SmallInt;
                break;
            case "guid":
                gbsType = GbsType.Guid;
                break;
            case "blob":
                gbsType = GbsType.Blob;
                break;
            case "clob":
                gbsType = GbsType.Clob;
                break;
            case "text":
                gbsType = GbsType.Text;
                break;
            case "byte":
                gbsType = GbsType.Byte;
                break;
            case "float":
                gbsType = GbsType.Float;
                break;
            case "smallfloat":
                gbsType = GbsType.SmallFloat;
                break;
            default:
                if (column_type.ToLower().Contains("datetime"))
                {
                    gbsType = GbsType.DateTime;
                    break;
                }
                throw new ArgumentException(string.Format("Invalid column type: {0}", column_type));
        }

        return;
    }

    internal static void GetParameterInfo(TableInfo tableInfo, DataTable schema, string propertyName, out GbsType gbsType, out int column_size, out string? column_name)
    {
        bool rc = tableInfo.PropertyColumnNamesDict.TryGetValue(propertyName, out column_name);
        if (rc == false)
        {
            bool rc2 = tableInfo.PropertyColumnNamesDict.TryGetValue(propertyName.Replace("_", "."), out column_name);
            if (rc2 == false)
            {
                throw new ArgumentException($"Unable to get column name based on propertyname: {propertyName}");
            }
        }

        DataRow? schema_row = schema.Rows.Find(column_name);
        if (schema_row == null)
        {
            throw new ArgumentException($"Unable to get schema info base on column name: {column_name}");
        }

        column_size = 0;
        gbsType = (GbsType)schema_row["ProviderType"];
        switch (gbsType)
        {
            case GbsType.Char1:
            case GbsType.Char:
            case GbsType.NChar:
            case GbsType.VarChar:
            case GbsType.NVarChar:
            case GbsType.LVarChar:
                // assigned column size for char type
                column_size = (int)schema_row["ColumnSize"];
                break;
        }

        return;
    }

    private static DataTable GetSchemaTable(GbsConnection cn, GbsTransaction? transaction, TableInfo tableInfo)
    {
        var table_name = tableInfo.InsertToTempTable ? tableInfo.TempTableName : tableInfo.TableName;
        DataTable schema = new DataTable();
        schema.Columns.Add(new DataColumn("ColumnName", typeof(System.String)));
        schema.Columns.Add(new DataColumn("ColumnSize", typeof(System.Int32)));
        schema.Columns.Add(new DataColumn("ProviderType", typeof(System.Int32)));
        schema.PrimaryKey = new DataColumn[] { schema.Columns[0] };

        using (var cmd_meta = cn.CreateCommand())
        {
            StringBuilder sb = new StringBuilder();
            sb.Append("SELECT c.colname, c.coltype, c.collength, c.extended_id, ext.name ");
            sb.Append("FROM systables t ");
            sb.Append("INNER JOIN syscolumns c ON t.tabid = c.tabid ");
            sb.Append("LEFT JOIN sysxtdtypes ext ON ext.extended_id = c.extended_id ");
            sb.Append("WHERE tabname = '" + table_name!.ToLower() + "'");

            cmd_meta.CommandText = sb.ToString();
            cmd_meta.Transaction = (GbsTransaction)transaction!;
            var reader = cmd_meta.ExecuteReader();

            while (reader.Read())
            {
                DataRow row = schema.NewRow();
                row["ColumnName"] = reader.GetString(0);
                row["ColumnSize"] = reader.GetInt32(2);

                GbsType provider_type = (GbsType)(reader.GetInt16(1) & 0xFF);
                int extended_type = reader.GetInt32(3);
                switch ((GbsType)provider_type)
                {
                    case GbsType.SQLUDTFixed:
                        switch (extended_type)
                        {
                            case 11:
                                provider_type = GbsType.Clob;
                                break;
                            case 10:
                                provider_type = GbsType.Blob;
                                break;
                            case 5:
                                provider_type = GbsType.Boolean;
                                break;
                        }
                        break;
                    case GbsType.SQLUDTVar:
                        switch (extended_type)
                        {
                            case 1:
                                provider_type = GbsType.LVarChar;
                                break;
                        }
                        break;
                    case GbsType.Char1:
                        provider_type = GbsType.Char;
                        break;
                    case GbsType.BigSerial:
                        provider_type = GbsType.BigInt;
                        break;
                    case GbsType.Serial:
                        provider_type = GbsType.Integer;
                        break;
                    default:
                        switch (reader.GetString(4))
                        {
                            case "guid":
                                provider_type = GbsType.Guid;
                                break;
                        }
                        break;
                }
                row["ProviderType"] = provider_type;
                schema.Rows.Add(row);
            }
        }

        return schema;
    }

    internal static GbsCommand GetGBaseCommand<T>(BulkContext context, Type? type, IEnumerable<T> entities, TableInfo tableInfo, GbsConnection connection, GbsTransaction? transaction)
    {
        string? column_name = string.Empty;
        int column_size = 0;
        var dbContext = context.DbContext;
        GbsType gbsType = GbsType.Char;

        GbsCommand command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandType = CommandType.Text;

        var operationType = tableInfo.BulkConfig.OperationType;
        switch (operationType)
        {
            case OperationType.Insert:
                command.CommandText = GBaseQueryBuilder.InsertIntoTable(tableInfo, OperationType.Insert);
                command.UpdatedRowSource = UpdateRowSource.None;

                break;
            case OperationType.InsertOrUpdate:
                command.CommandText = GBaseQueryBuilder.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);

                break;
            case OperationType.InsertOrUpdateOrDelete:
                throw new NotSupportedException("'BulkInsertOrUpdateDelete' not supported for GBase. "
                                                + " Another way to achieve this is to BulkRead existing data from DB, split list into sublists and call separately Bulk methods for Insert, Update, Delete.");
            case OperationType.Update:
                //command.CommandText = GBaseQueryBuilder.UpdateSetTable(tableInfo);
                command.CommandText = GBaseQueryBuilder.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);

                break;
            case OperationType.Delete:
                //command.CommandText = GBaseQueryBuilder.DeleteFromTable(tableInfo);
                command.CommandText = GBaseQueryBuilder.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);

                break;
        }

        type = tableInfo.HasAbstractList ? entities.ElementAt(0)?.GetType() : type;
        if (type is null)
        {
            throw new ArgumentException("Unable to determine entity type");
        }

        DataTable schema = GBaseAdapter.GetSchemaTable(connection, transaction, tableInfo);

        var entityType = dbContext.Model.FindEntityType(type);

        var entityPropertiesDict = entityType?.GetProperties().Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name)).ToDictionary(a => a.Name, a => a);
        //var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

        //var entityShadowFkPropertiesDict = entityType?.GetProperties().Where(
        //    a => a.IsShadowProperty() &&
        //         a.IsForeignKey() &&
        //         a.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.Name != null)
        //   .ToDictionary(x => x.GetContainingForeignKeys()?.First()?.DependentToPrincipal?.Name ?? string.Empty, a => a);

        //foreach (var property in properties)
        foreach (var propertyEntityType in entityPropertiesDict!.Values)
        {
            //IProperty? propertyEntityType = null;
            //if (entityPropertiesDict?.ContainsKey(property.Name) ?? false)
            //{
            //    propertyEntityType = entityPropertiesDict[property.Name];
            //}
            //else if (entityShadowFkPropertiesDict?.ContainsKey(property.Name) ?? false)
            //{
            //    propertyEntityType = entityShadowFkPropertiesDict[property.Name];
            //}

            if (propertyEntityType != null)
            {
                if (operationType == OperationType.Insert &&
                    tableInfo.DefaultValueProperties.Contains(propertyEntityType.Name))
                {
                    continue;
                }

                GBaseAdapter.GetParameterInfo(tableInfo, schema, propertyEntityType.Name, out gbsType, out column_size, out column_name);
                var parameter = new GbsParameter($"{propertyEntityType.Name}", gbsType, column_size, $"{column_name}");

                command.Parameters.Add(parameter);
            }
        }

        var propertyColumnsDict = tableInfo.PropertyColumnNamesDict;
        foreach (var propertyColumn in propertyColumnsDict)
        {
            var isShadowProperty = tableInfo.ShadowProperties.Contains(propertyColumn.Key);
            string parameterName = propertyColumn.Key.Replace(".", "_");

            if (operationType == OperationType.Insert &&
                tableInfo.DefaultValueProperties.Contains(parameterName))
            {
                continue;
            }
            if (!isShadowProperty)
            {
                if (propertyColumn.Key.Contains('.')) // ToDo: change IF clause to check for NavigationProperties, optimise, integrate with same code segment from LoadData method
                {
                    var ownedPropertyNameList = propertyColumn.Key.Split('.');
                    var ownedPropertyName = ownedPropertyNameList[0];
                    var ownedFastProperty = tableInfo.FastPropertyDict[ownedPropertyName];
                    var ownedProperty = ownedFastProperty.Property;

                    var propertyType = Nullable.GetUnderlyingType(ownedProperty.GetType()) ?? ownedProperty.GetType();

                    if (!command.Parameters.Contains(parameterName))
                    {
                        GBaseAdapter.GetParameterInfo(tableInfo, schema, parameterName, out gbsType, out column_size, out column_name);
                        var parameter = new GbsParameter($"{parameterName}", gbsType, column_size, $"{column_name}");
                        //var parameter = new GbsParameter($"{parameterName}", propertyType);
                        command.Parameters.Add(parameter);
                    }
                }
            }
        }

        return command;
    }

    internal static void LoadGBaseValues<T>(TableInfo tableInfo, T? entity, GbsCommand command, DbContext dbContext, OperationType operationType)
    {
        var propertyColumnsDict = tableInfo.PropertyColumnNamesDict;

        foreach (var propertyColumn in propertyColumnsDict)
        {
            var isShadowProperty = tableInfo.ShadowProperties.Contains(propertyColumn.Key);
            string parameterName = propertyColumn.Key.Replace(".", "_");
            object? value = null;

            if (operationType == OperationType.Insert &&
                tableInfo.DefaultValueProperties.Contains(parameterName))
            {
                continue;
            }
            if (!isShadowProperty)
            {
                if (propertyColumn.Key.Contains('.')) // ToDo: change IF clause to check for NavigationProperties, optimise, integrate with same code segment from LoadData method
                {
                    var ownedPropertyNameList = propertyColumn.Key.Split('.');
                    var ownedPropertyName = ownedPropertyNameList[0];
                    var subPropertyName = ownedPropertyNameList[1];
                    var ownedFastProperty = tableInfo.FastPropertyDict[ownedPropertyName];
                    var ownedProperty = ownedFastProperty.Property;

                    if (ownedProperty == null)
                    {
                        value = null;
                    }
                    else
                    {
                        var ownedPropertyValue = entity == null ? null : tableInfo.FastPropertyDict[ownedPropertyName].Get(entity);
                        var subPropertyFullName = $"{ownedPropertyName}_{subPropertyName}";
                        value = ownedPropertyValue == null ? null : tableInfo.FastPropertyDict[subPropertyFullName]?.Get(ownedPropertyValue);
                        //if (ownedPropertyNameList.Count() == 3)
                        //{
                        //    // support three level owned table.
                        //    value = value == null ? null : tableInfo.FastPropertyDict[propertyColumn.Key.Replace(".", "_")]?.Get(value);
                        //}
                    }
                }
                else if (tableInfo.FastPropertyDict.ContainsKey(propertyColumn.Key))
                {
                    value = entity is null ? null : tableInfo.FastPropertyDict[propertyColumn.Key].Get(entity);
                }
                else if (tableInfo.ColumnToPropertyDictionary.ContainsKey(propertyColumn.Key))
                {
                    var property = tableInfo.ColumnToPropertyDictionary[propertyColumn.Key];

                    if (property.IsShadowProperty() && property.IsForeignKey())
                    {
                        var foreignKey = property.GetContainingForeignKeys().FirstOrDefault();
                        var principalNavigation = foreignKey?.DependentToPrincipal;
                        var pkPropertyName = foreignKey?.PrincipalKey.Properties.FirstOrDefault()?.Name;

                        if (principalNavigation is not null && pkPropertyName is not null)
                        {
                            pkPropertyName = principalNavigation.Name + "_" + pkPropertyName;
                            var fkPropertyValue = entity == null ? null : tableInfo.FastPropertyDict[principalNavigation.Name].Get(entity);
                            value = fkPropertyValue == null ? null : tableInfo.FastPropertyDict[pkPropertyName]?.Get(fkPropertyValue);
                        }
                    }
                }
            }
            else
            {
                if (tableInfo.BulkConfig.EnableShadowProperties)
                {
                    if (tableInfo.BulkConfig.ShadowPropertyValue == null)
                    {
                        value = entity is null ? null : dbContext.Entry(entity).Property(propertyColumn.Key).CurrentValue; // Get the shadow property value
                    }
                    else
                    {
                        value = entity is null ? null : tableInfo.BulkConfig.ShadowPropertyValue(entity, propertyColumn.Key);
                    }
                }
                else
                {
                    value = entity is null ? null : dbContext.Entry(entity).Metadata.GetDiscriminatorValue(); // Set the value for the discriminator column
                }
            }

            if (tableInfo.ConvertibleColumnConverterDict.ContainsKey(propertyColumn.Value) && value != DBNull.Value)
            {
                value = tableInfo.ConvertibleColumnConverterDict[propertyColumn.Value].ConvertToProvider.Invoke(value);
            }

            command.Parameters[$"{parameterName}"].Value = value ?? DBNull.Value;
        }
    }
    /// <summary>
    /// Common logic for two versions of GetDataTable
    /// </summary>
    private static DataTable InnerGetDataTable<T>(BulkContext context, ref Type type, IEnumerable<T> entities, TableInfo tableInfo)
    {
        var dbContext = context.DbContext;
        var dataTable = new DataTable();
        var columnsDict = new Dictionary<string, object?>();
        var ownedEntitiesMappedProperties = new HashSet<string>();

        var databaseType = context.Server.Type;
        var isSqlServer = databaseType == SqlType.SqlServer;

        var objectIdentifier = tableInfo.ObjectIdentifier;
        type = tableInfo.HasAbstractList ? entities.ElementAt(0)!.GetType() : type;
        var entityType = dbContext.Model.FindEntityType(type) ?? throw new ArgumentException($"Unable to determine entity type from given type - {type.Name}");
        var entityTypeProperties = entityType.GetProperties().ToList();
        var entityPropertiesDict = entityTypeProperties.Where(a => tableInfo.PropertyColumnNamesDict.ContainsKey(a.Name) ||
                                                                   (tableInfo.BulkConfig.OperationType != OperationType.Read && a.Name == tableInfo.TimeStampPropertyName))
                                                       .ToDictionary(a => a.Name, a => a);
        var entityNavigationOwnedDict = entityType.GetNavigations().Where(a => a.TargetEntityType.IsOwned()).ToDictionary(a => a.Name, a => a);
        var entityShadowFkPropertiesDict = entityTypeProperties.Where(a => a.IsShadowProperty() &&
                                                                           a.IsForeignKey() &&
                                                                           a.GetContainingForeignKeys().FirstOrDefault()?.DependentToPrincipal?.Name != null)
                                                                     .ToDictionary(x => x.GetContainingForeignKeys()?.First()?.DependentToPrincipal?.Name ?? string.Empty, a => a);

        var entityShadowFkPropertyColumnNamesDict = entityShadowFkPropertiesDict
            .ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));
        var shadowPropertyColumnNamesDict = entityPropertiesDict
            .Where(a => a.Value.IsShadowProperty()).ToDictionary(a => a.Key, a => a.Value.GetColumnName(objectIdentifier));

        var properties = type.GetProperties(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        var discriminatorColumn = GetDiscriminatorColumn(tableInfo);

        foreach (var property in properties)
        {
            var hasDefaultValueOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                && !tableInfo.BulkConfig.SetOutputIdentity
                && tableInfo.DefaultValueProperties.Contains(property.Name);
            
            if (entityPropertiesDict.TryGetValue(property.Name, out var propertyEntityType))
            {
                string columnName = propertyEntityType.GetColumnName(objectIdentifier) ?? string.Empty;

                var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName].ProviderClrType : property.PropertyType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (!columnsDict.ContainsKey(property.Name) && !hasDefaultValueOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(property.Name, null);
                }
            }
            else if (entityShadowFkPropertiesDict.TryGetValue(property.Name, out var fk))
            {
                entityPropertiesDict.TryGetValue(fk.GetColumnName(objectIdentifier) ?? string.Empty, out var entityProperty);
                if (entityProperty == null) // BulkRead
                    continue;

                var columnName = entityProperty.GetColumnName(objectIdentifier);

                var isConvertible = tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName ?? string.Empty);
                var propertyType = isConvertible ? tableInfo.ConvertibleColumnConverterDict[columnName ?? string.Empty].ProviderClrType : entityProperty.ClrType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                if (propertyType == typeof(Geometry) && isSqlServer)
                {
                    propertyType = typeof(byte[]);
                }

                if (columnName is not null && !(columnsDict.ContainsKey(columnName)) && !hasDefaultValueOnInsert)
                {
                    dataTable.Columns.Add(columnName, propertyType);
                    columnsDict.Add(columnName, null);
                }
            }
            else if (entityNavigationOwnedDict.ContainsKey(property.Name)) // isOWned
            {
                var ownedEntityType = dbContext.Model.FindEntityType(property.PropertyType);
                if (ownedEntityType == null)
                {
                    ownedEntityType = dbContext.Model.GetEntityTypes().SingleOrDefault(x => x.ClrType == property.PropertyType && x.Name.StartsWith(entityType.Name + "." + property.Name + "#"));
                }

                var ownedEntityProperties = ownedEntityType?.GetProperties().ToList() ?? new();
                var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                foreach (var ownedEntityProperty in ownedEntityProperties)
                {
                    if (!ownedEntityProperty.IsPrimaryKey())
                    {
                        string? columnName = ownedEntityProperty.GetColumnName(objectIdentifier);
                        if (columnName is not null && tableInfo.PropertyColumnNamesDict.ContainsValue(columnName))
                        {
                            ownedEntityPropertyNameColumnNameDict.Add(ownedEntityProperty.Name, columnName);
                            ownedEntitiesMappedProperties.Add(property.Name + "_" + ownedEntityProperty.Name);
                        }
                    }
                }

                var innerProperties = property.PropertyType.GetProperties();
                if (!tableInfo.LoadOnlyPKColumn)
                {
                    foreach (var innerProperty in innerProperties)
                    {
                        if (ownedEntityPropertyNameColumnNameDict.TryGetValue(innerProperty.Name, out var columnName))
                        {
                            var propertyName = $"{property.Name}_{innerProperty.Name}";

                            if (tableInfo.ConvertibleColumnConverterDict.TryGetValue(propertyName, out var convertor))
                            {
                                var underlyingType = Nullable.GetUnderlyingType(convertor.ProviderClrType) ?? convertor.ProviderClrType;

                                dataTable.Columns.Add(columnName, underlyingType);
                            }
                            else
                            {
                                var ownedPropertyType = Nullable.GetUnderlyingType(innerProperty.PropertyType) ?? innerProperty.PropertyType;

                                dataTable.Columns.Add(columnName, ownedPropertyType);
                            }

                            columnsDict.Add(property.Name + "_" + innerProperty.Name, null);
                        }
                    }
                }
            }
        }

        if (tableInfo.BulkConfig.EnableShadowProperties)
        {
            foreach (var shadowProperty in entityPropertiesDict.Values.Where(a => a.IsShadowProperty()))
            {
                string? columnName = shadowProperty.GetColumnName(objectIdentifier);

                // If a model has an entity which has a relationship without an explicity defined FK, the data table will already contain the foreign key shadow property
                if (columnName is not null && dataTable.Columns.Contains(columnName))
                    continue;

                var isConvertible = columnName is not null && tableInfo.ConvertibleColumnConverterDict.ContainsKey(columnName);

                var propertyType = isConvertible
                    ? tableInfo.ConvertibleColumnConverterDict[columnName!].ProviderClrType
                    : shadowProperty.ClrType;

                var underlyingType = Nullable.GetUnderlyingType(propertyType);
                if (underlyingType != null)
                {
                    propertyType = underlyingType;
                }

                dataTable.Columns.Add(columnName, propertyType);
                columnsDict.Add(shadowProperty.Name, null);
            }
        }

        if (discriminatorColumn != null)
        {
            var discriminatorProperty = entityPropertiesDict[discriminatorColumn];

            dataTable.Columns.Add(discriminatorColumn, discriminatorProperty.ClrType);
            columnsDict.Add(discriminatorColumn, entityType.GetDiscriminatorValue());
        }

        bool hasConverterProperties = tableInfo.ConvertiblePropertyColumnDict.Count > 0;

        var index = 0;
        foreach (T entity in entities)
        {
            var propertiesToLoad = properties
                .Where(a => !tableInfo.AllNavigationsDictionary.ContainsKey(a.Name)
                            || entityShadowFkPropertiesDict.ContainsKey(a.Name)
                            || tableInfo.OwnedTypesDict.ContainsKey(a.Name)); // omit virtual Navigation (except Owned and ShadowNavig.) since it's Getter can cause unwanted Select-s from Db

            foreach (var property in propertiesToLoad)
            {
                object? propertyValue = tableInfo.FastPropertyDict.ContainsKey(property.Name)
                    ? tableInfo.FastPropertyDict[property.Name].Get(entity!)
                    : null;

                var hasDefaultVauleOnInsert = tableInfo.BulkConfig.OperationType == OperationType.Insert
                    && !tableInfo.BulkConfig.SetOutputIdentity
                    && tableInfo.DefaultValueProperties.Contains(property.Name);
                
                if (tableInfo.BulkConfig.DateTime2PrecisionForceRound
                    && isSqlServer
                    && tableInfo.DateTime2PropertiesPrecisionLessThen7Dict.TryGetValue(property.Name, out var precision))
                {
                    DateTime? dateTimePropertyValue = (DateTime?)propertyValue;

                    if (dateTimePropertyValue is not null)
                    {
                        int digitsToRemove = 7 - precision;
                        int powerOf10 = (int)Math.Pow(10, digitsToRemove);

                        long subsecondTicks = dateTimePropertyValue.Value.Ticks % 10000000;
                        long ticksToRound = subsecondTicks + (subsecondTicks % 10 == 0 ? 1 : 0); // if ends with 0 add 1 tick to make sure rounding of value .5_zeros is rounded to Upper like SqlServer is doing, not to Even as Math.Round works
                        int roundedTicks = Convert.ToInt32(Math.Round((decimal)ticksToRound / powerOf10, 0)) * powerOf10;
                        dateTimePropertyValue = dateTimePropertyValue.Value.AddTicks(-subsecondTicks).AddTicks(roundedTicks);

                        propertyValue = dateTimePropertyValue;
                    }
                }

                if (hasConverterProperties && tableInfo.ConvertiblePropertyColumnDict.TryGetValue(property.Name, out var convertibleColumnName))
                {
                    propertyValue = tableInfo.ConvertibleColumnConverterDict[convertibleColumnName].ConvertToProvider.Invoke(propertyValue);
                }

                if (entityPropertiesDict.ContainsKey(property.Name) && !hasDefaultVauleOnInsert)
                {
                    columnsDict[property.Name] = propertyValue;
                }
                else if (entityShadowFkPropertiesDict.TryGetValue(property.Name, out var foreignKeyShadowProperty))
                {
                    var columnName = entityShadowFkPropertyColumnNamesDict[property.Name] ?? string.Empty;
                    if (!entityPropertiesDict.TryGetValue(columnName, out var entityProperty) || entityProperty is null)
                    {
                        continue; // BulkRead
                    }

                    columnsDict[columnName] = propertyValue == null
                        ? null
                        : foreignKeyShadowProperty.FindFirstPrincipal()?.PropertyInfo?.GetValue(propertyValue); // TODO Check if can be optimized
                }
                else if (entityNavigationOwnedDict.ContainsKey(property.Name) && !tableInfo.LoadOnlyPKColumn)
                {
                    var ownedProperties = property.PropertyType.GetProperties().Where(a => ownedEntitiesMappedProperties.Contains(property.Name + "_" + a.Name));
                    foreach (var ownedProperty in ownedProperties)
                    {
                        var columnName = $"{property.Name}_{ownedProperty.Name}";
                        var ownedPropertyValue = propertyValue == null ? null : tableInfo.FastPropertyDict[columnName].Get(propertyValue);

                        if (tableInfo.ConvertibleColumnConverterDict.TryGetValue(columnName, out var converter))
                        {
                            columnsDict[columnName] = ownedPropertyValue == null ? null : converter.ConvertToProvider.Invoke(ownedPropertyValue);
                        }
                        else
                        {
                            columnsDict[columnName] = ownedPropertyValue;
                        }
                    }
                }
            }

            if (tableInfo.BulkConfig.EnableShadowProperties)
            {
                foreach (var shadowPropertyName in shadowPropertyColumnNamesDict.Keys)
                {
                    var columnName = shadowPropertyColumnNamesDict[shadowPropertyName] ?? string.Empty;

                    var propertyValue = default(object);

                    if (tableInfo.BulkConfig.ShadowPropertyValue == null)
                    {
                        propertyValue = dbContext.Entry(entity!).Property(shadowPropertyName).CurrentValue;
                    }
                    else
                    {
                        propertyValue = tableInfo.BulkConfig.ShadowPropertyValue(entity!, shadowPropertyName);
                    }

                    if (tableInfo.ConvertibleColumnConverterDict.TryGetValue(columnName, out var converter))
                    {
                        propertyValue = converter.ConvertToProvider.Invoke(propertyValue);
                    }

                    columnsDict[shadowPropertyName] = propertyValue;
                }
            }

            var record = columnsDict.Values.ToArray();

            dataTable.Rows.Add(record);
            index++;
        }

        return dataTable;
    }

    #region Connection
    internal static async Task<GbsConnection> OpenAndGetGBaseConnectionAsync(DbContext context, CancellationToken cancellationToken)
    {
        await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        return (GbsConnection)context.Database.GetDbConnection();
    }

    internal static GbsConnection OpenAndGetGBaseConnection(DbContext context)
    {
        context.Database.OpenConnection();

        return (GbsConnection)context.Database.GetDbConnection();
    }
    #endregion
    private static string? GetDiscriminatorColumn(TableInfo tableInfo)
    {
        string? discriminatorColumn = null;
        if (!tableInfo.BulkConfig.EnableShadowProperties && tableInfo.ShadowProperties.Count > 0)
        {
            var stringColumns = tableInfo.ColumnNamesTypesDict.Where(a => a.Value.Contains("char")).Select(a => a.Key).ToList();
            discriminatorColumn = tableInfo.ShadowProperties.Where(a => stringColumns.Contains(a)).ElementAt(0);
        }
        return discriminatorColumn;
    }
    #region GBaseData

    /// <inheritdoc/>
    public static void SetIdentityForOutput<T>(IEnumerable<T> entities, TableInfo tableInfo, object? lastRowIdScalar)
    {
        long counter = (long?)lastRowIdScalar ?? 0;

        string identityPropertyName = tableInfo.PropertyColumnNamesDict.SingleOrDefault(a => a.Value == tableInfo.IdentityColumnName).Key;
        FastProperty identityFastProperty = tableInfo.FastPropertyDict[identityPropertyName];

        string idTypeName = identityFastProperty.Property.PropertyType.Name;
        object? idValue = null;
        for (int i = entities.Count() - 1; i >= 0; i--)
        {
            idValue = idTypeName switch
            {
                "Int64" => counter, // long is default
                "UInt64" => (ulong)counter,
                "Int32" => (int)counter,
                "UInt32" => (uint)counter,
                "Int16" => (short)counter,
                "UInt16" => (ushort)counter,
                "Byte" => (byte)counter,
                "SByte" => (sbyte)counter,
                _ => counter,
            };
            if (entities.ElementAt(i) is not null)
            {
                identityFastProperty.Set(entities.ElementAt(i)!, idValue);
            }

            counter--;
        }
    }
    #endregion
}
