using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using FastMember;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.BulkExtensions
{
    public class TableInfo
    {
        public string Schema { get; set; }
        public string SchemaFormated => Schema != null ? $"[{Schema}]." : "";
        public string TableName { get; set; }
        public string FullTableName => $"{SchemaFormated}[{TableName}]";
        public List<string> PrimaryKeys { get; set; }
        public bool HasSinglePrimaryKey { get; set; }
        public bool UpdateByPropertiesAreNullable { get; set; }

        protected string TempDBPrefix => BulkConfig.UseTempDB ? "#" : "";
        public string TempTableSufix { get; set; }
        public string TempTableName => $"{TableName}{TempTableSufix}";
        public string FullTempTableName => $"{SchemaFormated}[{TempDBPrefix}{TempTableName}]";
        public string FullTempOutputTableName => $"{SchemaFormated}[{TempDBPrefix}{TempTableName}Output]";

        public bool InsertToTempTable { get; set; }
        public bool HasIdentity { get; set; }
        public bool HasOwnedTypes { get; set; }
        public int NumberOfEntities { get; set; }

        public BulkConfig BulkConfig { get; set; }
        public Dictionary<string, string> OutputPropertyColumnNamesDict { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> PropertyColumnNamesDict { get; set; } = new Dictionary<string, string>();
        public HashSet<string> ShadowProperties { get; set; } = new HashSet<string>();
        public Dictionary<string, ValueConverter> ConvertibleProperties { get; set; } = new Dictionary<string, ValueConverter>();
        public string TimeStampColumn { get; set; }

        public static TableInfo CreateInstance<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig)
        {
            var tableInfo = new TableInfo
            {
                NumberOfEntities = entities.Count,
                BulkConfig = bulkConfig ?? new BulkConfig()
            };

            bool isExplicitTransaction = context.Database.GetDbConnection().State == ConnectionState.Open;
            if (tableInfo.BulkConfig.UseTempDB == true && !isExplicitTransaction && operationType != OperationType.Insert)
            {
                tableInfo.BulkConfig.UseTempDB = false;
                // If BulkOps is not in explicit transaction then tempdb[#] can only be used with Insert, other Operations done with customTemp table.
                // Otherwise throws exception: 'Cannot access destination table' (gets Droped too early because transaction ends before operation is finished)
            }

            var isDeleteOperation = operationType == OperationType.Delete;
            tableInfo.LoadData<T>(context, isDeleteOperation);
            return tableInfo;
        }

        public void LoadData<T>(DbContext context, bool loadOnlyPKColumn)
        {
            var entityType = context.Model.FindEntityType(typeof(T));
            if (entityType == null)
                throw new InvalidOperationException("DbContext does not contain EntitySet for Type: " + typeof(T).Name);

            var relationalData = entityType.Relational();
            Schema = relationalData.Schema ?? "dbo";
            TableName = relationalData.TableName;
            TempTableSufix = "Temp" + Guid.NewGuid().ToString().Substring(0, 8); // 8 chars of Guid as tableNameSufix to avoid same name collision with other tables

            bool AreSpecifiedUpdateByProperties = BulkConfig.UpdateByProperties?.Count() > 0;
            PrimaryKeys = AreSpecifiedUpdateByProperties ? BulkConfig.UpdateByProperties : entityType.FindPrimaryKey().Properties.Select(a => a.Name).ToList();
            HasSinglePrimaryKey = PrimaryKeys.Count == 1;

            var allProperties = entityType.GetProperties().AsEnumerable();

            var allNavigationProperties = entityType.GetNavigations().Where(a => a.GetTargetType().IsOwned());
            HasOwnedTypes = allNavigationProperties.Any();

            // timestamp datatype can only be set by database, that's property having [Timestamp] Attribute but keep if one with [ConcurrencyCheck]
            var timeStampProperties = allProperties.Where(a => a.IsConcurrencyToken == true && a.ValueGenerated == ValueGenerated.OnAddOrUpdate && a.BeforeSaveBehavior == PropertySaveBehavior.Ignore);
            TimeStampColumn = timeStampProperties.FirstOrDefault()?.Relational().ColumnName; // expected to be only One
            var properties = allProperties.Except(timeStampProperties);

            OutputPropertyColumnNamesDict = properties.ToDictionary(a => a.Name, b => b.Relational().ColumnName);

            properties = properties.Where(a => a.Relational().ComputedColumnSql == null);

            bool AreSpecifiedPropertiesToInclude = BulkConfig.PropertiesToInclude?.Count() > 0;
            bool AreSpecifiedPropertiesToExclude = BulkConfig.PropertiesToExclude?.Count() > 0;

            if (AreSpecifiedPropertiesToInclude)
            {
                if (AreSpecifiedUpdateByProperties) // Adds UpdateByProperties to PropertyToInclude if they are not already explicitly listed
                {
                    foreach (var updateByProperty in BulkConfig.UpdateByProperties)
                    {
                        if (!BulkConfig.PropertiesToInclude.Contains(updateByProperty))
                        {
                            BulkConfig.PropertiesToInclude.Add(updateByProperty);
                        }
                    }
                }
                else // Adds PrimaryKeys to PropertyToInclude if they are not already explicitly listed
                {
                    foreach (var primaryKey in PrimaryKeys)
                    {
                        if (!BulkConfig.PropertiesToInclude.Contains(primaryKey))
                        {
                            BulkConfig.PropertiesToInclude.Add(primaryKey);
                        }
                    }
                }
            }

            UpdateByPropertiesAreNullable = properties.Any(a => PrimaryKeys.Contains(a.Name) && a.IsNullable);

            if (AreSpecifiedPropertiesToInclude || AreSpecifiedPropertiesToExclude)
            {
                if (AreSpecifiedPropertiesToInclude && AreSpecifiedPropertiesToExclude)
                    throw new InvalidOperationException("Only one group of properties, either PropertiesToInclude or PropertiesToExclude can be specifed, specifying both not allowed.");
                if (AreSpecifiedPropertiesToInclude)
                    properties = properties.Where(a => BulkConfig.PropertiesToInclude.Contains(a.Name));
                if (AreSpecifiedPropertiesToExclude)
                    properties = properties.Where(a => !BulkConfig.PropertiesToExclude.Contains(a.Name));
            }

            if (loadOnlyPKColumn)
            {
                PropertyColumnNamesDict = properties.Where(a => PrimaryKeys.Contains(a.Name)).ToDictionary(a => a.Name, b => b.Relational().ColumnName);
            }
            else
            {
                PropertyColumnNamesDict = properties.ToDictionary(a => a.Name, b => b.Relational().ColumnName);
                ShadowProperties = new HashSet<string>(properties.Where(p => p.IsShadowProperty).Select(p => p.Relational().ColumnName));
                foreach (var property in properties.Where(p => p.GetValueConverter() != null))
                    ConvertibleProperties.Add(property.Relational().ColumnName, property.GetValueConverter());
            }
        }

        public void CheckHasIdentity(DbContext context)
        {
            int hasIdentity = 0;
            if (HasSinglePrimaryKey)
            {
                var sqlConnection = context.Database.GetDbConnection();
                var currentTransaction = context.Database.CurrentTransaction;
                try
                {
                    if (currentTransaction == null)
                    {
                        if (sqlConnection.State != ConnectionState.Open)
                            sqlConnection.Open();
                    }
                    using (var command = sqlConnection.CreateCommand())
                    {
                        if (currentTransaction != null)
                            command.Transaction = currentTransaction.GetDbTransaction();
                        command.CommandText = SqlQueryBuilder.SelectIsIdentity(FullTableName, PropertyColumnNamesDict[PrimaryKeys[0]]);
                        using (var reader = command.ExecuteReader())
                        {
                            if (reader.HasRows)
                            {
                                while (reader.Read())
                                {
                                    hasIdentity = reader[0] == DBNull.Value ? 0 : (int)reader[0];
                                }
                            }
                        }
                    }
                }
                finally
                {
                    if (currentTransaction == null)
                        sqlConnection.Close();
                }
            }
            HasIdentity = hasIdentity == 1;
        }

        public async Task CheckHasIdentityAsync(DbContext context)
        {
            int hasIdentity = 0;
            if (HasSinglePrimaryKey)
            {
                var sqlConnection = context.Database.GetDbConnection();
                var currentTransaction = context.Database.CurrentTransaction;
                try
                {
                    if (currentTransaction == null)
                    {
                        if (sqlConnection.State != ConnectionState.Open)
                            await sqlConnection.OpenAsync().ConfigureAwait(false);
                    }
                    using (var command = sqlConnection.CreateCommand())
                    {
                        if (currentTransaction != null)
                            command.Transaction = currentTransaction.GetDbTransaction();
                        command.CommandText = SqlQueryBuilder.SelectIsIdentity(FullTableName, PropertyColumnNamesDict[PrimaryKeys[0]]);
                        using (var reader = await command.ExecuteReaderAsync().ConfigureAwait(false))
                        {
                            if (reader.HasRows)
                            {
                                while (await reader.ReadAsync().ConfigureAwait(false))
                                {
                                    hasIdentity = (int)reader[0];
                                }
                            }
                        }
                    }
                }
                finally
                {
                    if (currentTransaction == null)
                    {
                        sqlConnection.Close();
                    }
                }
            }
            HasIdentity = hasIdentity == 1;
        }

        public void SetSqlBulkCopyConfig<T>(SqlBulkCopy sqlBulkCopy, IList<T> entities, bool setColumnMapping, Action<decimal> progress)
        {
            sqlBulkCopy.DestinationTableName = InsertToTempTable ? FullTempTableName : FullTableName;
            sqlBulkCopy.BatchSize = BulkConfig.BatchSize;
            sqlBulkCopy.NotifyAfter = BulkConfig.NotifyAfter ?? BulkConfig.BatchSize;
            sqlBulkCopy.SqlRowsCopied += (sender, e) => {
                progress?.Invoke((decimal)(e.RowsCopied * 10000 / entities.Count) / 10000); // round to 4 decimal places
            };
            sqlBulkCopy.BulkCopyTimeout = BulkConfig.BulkCopyTimeout ?? sqlBulkCopy.BulkCopyTimeout;
            sqlBulkCopy.EnableStreaming = BulkConfig.EnableStreaming;

            if (setColumnMapping)
            {
                foreach (var element in PropertyColumnNamesDict)
                {
                    sqlBulkCopy.ColumnMappings.Add(element.Key, element.Value);
                }
            }
        }

        public void UpdateOutputIdentity<T>(DbContext context, IList<T> entities) where T : class
        {
            if (HasSinglePrimaryKey)
            {
                var entitiesWithOutputIdentity = QueryOutputTable<T>(context).ToList();
                UpdateEntitiesIdentity(entities, entitiesWithOutputIdentity);
            }
        }

        public async Task UpdateOutputIdentityAsync<T>(DbContext context, IList<T> entities) where T : class
        {
            if (HasSinglePrimaryKey)
            {
                var entitiesWithOutputIdentity = (await QueryOutputTableAsync<T>(context).ConfigureAwait(false)).ToList();
                UpdateEntitiesIdentity(entities, entitiesWithOutputIdentity);
            }
        }
        

        protected IEnumerable<T> QueryOutputTable<T>(DbContext context) where T : class
        {
            var compiled = EF.CompileQuery(GetQueryExpression<T>());
            var result = compiled(context);
            return result;
        }

        protected Task<IEnumerable<T>> QueryOutputTableAsync<T>(DbContext context) where T : class
        {
            var compiled = EF.CompileAsyncQuery(GetQueryExpression<T>());
            var result = compiled(context);
            return result;
        }

        private Expression<Func<DbContext, IEnumerable<T>>> GetQueryExpression<T>() where T : class
        {
            string q = SqlQueryBuilder.SelectFromOutputTable(this);
            Expression<Func<DbContext, IEnumerable<T>>> expr = (ctx) => ctx.Set<T>().FromSql(q).AsNoTracking();
            var ordered = OrderBy(expr, PrimaryKeys[0]);

            // ALTERNATIVELY OrderBy with DynamicLinq ('using System.Linq.Dynamic.Core;' NuGet required) that eliminates need for custom OrderBy<T> method with Expression.
            //var queryOrdered = query.OrderBy(PrimaryKeys[0]);

            return ordered;
        }

        protected void UpdateEntitiesIdentity<T>(IList<T> entities, IList<T> entitiesWithOutputIdentity)
        {
            if (BulkConfig.PreserveInsertOrder) // Updates PK in entityList
            {
                var accessor = TypeAccessor.Create(typeof(T), true);
                for (int i = 0; i < NumberOfEntities; i++)
                    accessor[entities[i], PrimaryKeys[0]] = accessor[entitiesWithOutputIdentity[i], PrimaryKeys[0]];
            }
            else // Clears entityList and then refills it with loaded entites from Db
            {
                entities.Clear();
                ((List<T>)entities).AddRange(entitiesWithOutputIdentity);
            }
        }

        private static Expression<Func<DbContext, IEnumerable<T>>> OrderBy<T>(Expression<Func<DbContext, IEnumerable<T>>> source, string ordering)
        {
            Type entityType = typeof(T);
            PropertyInfo property = entityType.GetProperty(ordering);
            ParameterExpression parameter = Expression.Parameter(entityType);
            MemberExpression propertyAccess = Expression.MakeMemberAccess(parameter, property);
            LambdaExpression orderByExp = Expression.Lambda(propertyAccess, parameter);
            MethodCallExpression resultExp = Expression.Call(typeof(Queryable), "OrderBy", new Type[] { entityType, property.PropertyType }, source.Body, Expression.Quote(orderByExp));
            return Expression.Lambda<Func<DbContext, IEnumerable<T>>>(resultExp, source.Parameters);
        }
    }
}
