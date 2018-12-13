using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using FastMember;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Query;
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

        public bool CreatedOutputTable => (BulkConfig.SetOutputIdentity && HasSinglePrimaryKey) || BulkConfig.CalculateStats;

        public bool InsertToTempTable { get; set; }
        public bool HasIdentity { get; set; }
        public bool HasOwnedTypes { get; set; }
        public int NumberOfEntities { get; set; }

        public BulkConfig BulkConfig { get; set; }
        public Dictionary<string, string> OutputPropertyColumnNamesDict { get; set; } = new Dictionary<string, string>();
        public Dictionary<string, string> PropertyColumnNamesDict { get; set; } = new Dictionary<string, string>();
        public HashSet<string> ShadowProperties { get; set; } = new HashSet<string>();
        public Dictionary<string, ValueConverter> ConvertibleProperties { get; set; } = new Dictionary<string, ValueConverter>();
        public string TimeStampOutColumnType => "varbinary(8)";
        public string TimeStampColumnName { get; set; }

        public static TableInfo CreateInstance<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig)
        {
            var tableInfo = new TableInfo
            {
                NumberOfEntities = entities.Count,
                BulkConfig = bulkConfig ?? new BulkConfig()
            };

            bool isExplicitTransaction = context.Database.GetDbConnection().State == ConnectionState.Open;
            if (tableInfo.BulkConfig.UseTempDB == true && !isExplicitTransaction && (operationType != OperationType.Insert || tableInfo.BulkConfig.SetOutputIdentity))
            {
                tableInfo.BulkConfig.UseTempDB = false;
                // If BulkOps is not in explicit transaction then tempdb[#] can only be used with Insert, other Operations done with customTemp table.
                // Otherwise throws exception: 'Cannot access destination table' (gets Droped too early because transaction ends before operation is finished)
            }

            var isDeleteOperation = operationType == OperationType.Delete;
            tableInfo.LoadData<T>(context, isDeleteOperation);
            return tableInfo;
        }

        #region Main
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
            var primaryKeys = entityType.FindPrimaryKey().Properties.Select(a => a.Name).ToList();
            HasSinglePrimaryKey = primaryKeys.Count == 1;
            PrimaryKeys = AreSpecifiedUpdateByProperties ? BulkConfig.UpdateByProperties : primaryKeys;

            var allProperties = entityType.GetProperties().AsEnumerable();

            var allNavigationProperties = entityType.GetNavigations().Where(a => a.GetTargetType().IsOwned());
            HasOwnedTypes = allNavigationProperties.Any();

            // timestamp/row version properties are only set by the Db, the property has a [Timestamp] Attribute or is configured in in FluentAPI with .IsRowVersion()
            // They can be identified by the columne type "timestamp" or .IsConcurrencyToken in combination with .ValueGenerated == ValueGenerated.OnAddOrUpdate
            string timestampDbTypeName = nameof(TimestampAttribute).Replace("Attribute", "").ToLower(); // = "timestamp";
            var timeStampProperties = allProperties.Where(a => (a.IsConcurrencyToken && a.ValueGenerated == ValueGenerated.OnAddOrUpdate) || a.Relational().ColumnType == timestampDbTypeName);
            TimeStampColumnName = timeStampProperties.FirstOrDefault()?.Relational().ColumnName; // can be only One
            var properties = allProperties.Except(timeStampProperties);
            properties = properties.Where(a => a.Relational().ComputedColumnSql == null); // a.ValueGenerated == ValueGenerated.OnAddOrUpdate // CONSIDER only Add or only Update

            OutputPropertyColumnNamesDict = allProperties.ToDictionary(a => a.Name, b => b.Relational().ColumnName);

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
                
                if (HasOwnedTypes)  // Support owned entity property update. TODO: Optimize
                {
                    var type = typeof(T);
                    foreach (var navgationProperty in allNavigationProperties)
                    {
                        var property = navgationProperty.PropertyInfo;
                        Type navOwnedType = type.Assembly.GetType(property.PropertyType.FullName);
                        var ownedEntityType = context.Model.FindEntityType(property.PropertyType);
                        if (ownedEntityType == null) // when entity has more then one ownedType (e.g. Address HomeAddress, Address WorkAddress) or one ownedType is in multiple Entities like Audit is usually.
                        {
                            ownedEntityType = context.Model.GetEntityTypes().SingleOrDefault(a => a.DefiningNavigationName == property.Name && a.DefiningEntityType.Name == entityType.Name);
                        }
                        var ownedEntityProperties = ownedEntityType.GetProperties().ToList();
                        var ownedEntityPropertyNameColumnNameDict = new Dictionary<string, string>();

                        foreach (var ownedEntityProperty in ownedEntityProperties)
                        {
                            if (!ownedEntityProperty.IsPrimaryKey())
                            {
                                string columnName = ownedEntityProperty.Relational().ColumnName;
                                ownedEntityPropertyNameColumnNameDict.Add(ownedEntityProperty.Name, columnName);
                            }
                        }
                        var ownedProperties = property.PropertyType.GetProperties();
                        foreach (var ownedProperty in ownedProperties)
                        {
                            if (ownedEntityPropertyNameColumnNameDict.ContainsKey(ownedProperty.Name))
                            {
                                string columnName = ownedEntityPropertyNameColumnNameDict[ownedProperty.Name];
                                var ownedPropertyType = Nullable.GetUnderlyingType(ownedProperty.PropertyType) ?? ownedProperty.PropertyType;
                                PropertyColumnNamesDict.Add(property.Name + "." + ownedProperty.Name, columnName);
                                OutputPropertyColumnNamesDict.Add(property.Name + "." + ownedProperty.Name, columnName);
                            }
                        }
                    }
                 }
            }
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
        #endregion

        #region SqlCommands
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

        protected int GetNumberUpdated(DbContext context)
        {
            var resultParameter = new SqlParameter("@result", SqlDbType.Int) { Direction = ParameterDirection.Output };
            string sqlQueryCount = SqlQueryBuilder.SelectCountIsUpdateFromOutputTable(this);
            context.Database.ExecuteSqlCommand($"SET @result = ({sqlQueryCount});", resultParameter);
            return (int)resultParameter.Value;
        }

        protected async Task<int> GetNumberUpdatedAsync(DbContext context)
        {
            var resultParameter = new SqlParameter("@result", SqlDbType.Int) { Direction = ParameterDirection.Output };
            string sqlQueryCount = SqlQueryBuilder.SelectCountIsUpdateFromOutputTable(this);
            await context.Database.ExecuteSqlCommandAsync($"SET @result = ({sqlQueryCount});", resultParameter);
            return (int)resultParameter.Value;
        }

        #endregion

        public static string GetUniquePropertyValues<T>(T entity, List<string> propertiesNames, TypeAccessor accessor)
        {
            string result = String.Empty;
            foreach (var propertyName in propertiesNames)
            {
                result += accessor[entity, propertyName];
            }
            return result;
        }

        #region ReadProcedures
        public Dictionary<string, string> ConfigureBulkReadTableInfo(DbContext context)
        {
            InsertToTempTable = true;
            if (BulkConfig.UpdateByProperties == null || BulkConfig.UpdateByProperties.Count() == 0)
                CheckHasIdentity(context);

            var previousPropertyColumnNamesDict = PropertyColumnNamesDict;
            BulkConfig.PropertiesToInclude = PrimaryKeys;
            PropertyColumnNamesDict = PropertyColumnNamesDict.Where(a => PrimaryKeys.Contains(a.Key)).ToDictionary(i => i.Key, i => i.Value);
            return previousPropertyColumnNamesDict;
        }

        public void UpdateReadEntities<T>(IList<T> entities, IList<T> existingEntities)
        {
            List<string> propertyNames = PropertyColumnNamesDict.Keys.ToList();
            List<string> selectByPropertyNames = PropertyColumnNamesDict.Keys.Where(a => PrimaryKeys.Contains(a)).ToList();

            var accessor = TypeAccessor.Create(typeof(T), true);
            Dictionary<string, T> existingEntitiesDict = new Dictionary<string, T>();
            foreach (var existingEntity in existingEntities)
            {
                string uniqueProperyValues = GetUniquePropertyValues(existingEntity, selectByPropertyNames, accessor);
                existingEntitiesDict.Add(uniqueProperyValues, existingEntity);
            }

            for (int i = 0; i < NumberOfEntities; i++)
            {
                var entity = entities[i];
                string uniqueProperyValues = GetUniquePropertyValues(entity, selectByPropertyNames, accessor);
                if (existingEntitiesDict.ContainsKey(uniqueProperyValues))
                {
                    var existingEntity = existingEntitiesDict[uniqueProperyValues];

                    foreach (var propertyName in propertyNames)
                    {
                        accessor[entities[i], propertyName] = accessor[existingEntity, propertyName];
                    }
                }
            }
        }
        #endregion

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

        // Compiled queries created manually to avoid EF Memory leak bug when using EF with dynamic SQL:
        // https://github.com/borisdj/EFCore.BulkExtensions/issues/73
        // Once the following Issue gets fixed(expected in EF 3.0) this can be replaced with code segment: DirectQuery
        // https://github.com/aspnet/EntityFrameworkCore/issues/12905
        #region CompiledQuery
        public void LoadOutputData<T>(DbContext context, IList<T> entities) where T : class
        {
            if (BulkConfig.SetOutputIdentity && HasSinglePrimaryKey)
            {
                string sqlQuery = SqlQueryBuilder.SelectFromOutputTable(this);
                var entitiesWithOutputIdentity = QueryOutputTable<T>(context, sqlQuery).ToList();
                UpdateEntitiesIdentity(entities, entitiesWithOutputIdentity);
            }
            if (BulkConfig.CalculateStats)
            {
                string sqlQueryCount =  SqlQueryBuilder.SelectCountIsUpdateFromOutputTable(this);

                int numberUpdated = GetNumberUpdated(context);
                BulkConfig.StatsInfo = new StatsInfo
                {
                    StatsNumberUpdated = numberUpdated,
                    StatsNumberInserted = entities.Count - numberUpdated
                };
            }
        }

        public async Task LoadOutputDataAsync<T>(DbContext context, IList<T> entities) where T : class
        {
            if (BulkConfig.SetOutputIdentity && HasSinglePrimaryKey)
            {
                string sqlQuery = SqlQueryBuilder.SelectFromOutputTable(this);
                var entitiesWithOutputIdentity = await QueryOutputTableAsync<T>(context, sqlQuery).ToListAsync().ConfigureAwait(false);
                UpdateEntitiesIdentity(entities, entitiesWithOutputIdentity);
            }
            if (BulkConfig.CalculateStats)
            {
                int numberUpdated = await GetNumberUpdatedAsync(context);
                BulkConfig.StatsInfo = new StatsInfo
                {
                    StatsNumberUpdated = numberUpdated,
                    StatsNumberInserted = entities.Count - numberUpdated
                };
            }
        }
        
        protected IEnumerable<T> QueryOutputTable<T>(DbContext context, string sqlQuery) where T : class
        {
            var compiled = EF.CompileQuery(GetQueryExpression<T>(sqlQuery));
            var result = compiled(context);
            return result;
        }

        protected AsyncEnumerable<T> QueryOutputTableAsync<T>(DbContext context, string sqlQuery) where T : class
        {
            var compiled = EF.CompileAsyncQuery(GetQueryExpression<T>(sqlQuery));
            var result = compiled(context);
            return result;
        }

        public Expression<Func<DbContext, IQueryable<T>>> GetQueryExpression<T>(string sqlQuery) where T : class
        {
            Expression<Func<DbContext, IQueryable<T>>> expression = null;
            if (BulkConfig.TrackingEntities) // If Else can not be replaced with Ternary operator for Expression
            {
                expression = (ctx) => ctx.Set<T>().FromSql(sqlQuery);
            }
            else
            {
                expression = (ctx) => ctx.Set<T>().FromSql(sqlQuery).AsNoTracking();
            }
            var ordered = OrderBy(expression, PrimaryKeys[0]);

            // ALTERNATIVELY OrderBy with DynamicLinq ('using System.Linq.Dynamic.Core;' NuGet required) that eliminates need for custom OrderBy<T> method with Expression.
            //var queryOrdered = query.OrderBy(PrimaryKeys[0]);

            return ordered;
        }

        private static Expression<Func<DbContext, IQueryable<T>>> OrderBy<T>(Expression<Func<DbContext, IQueryable<T>>> source, string ordering)
        {
            Type entityType = typeof(T);
            PropertyInfo property = entityType.GetProperty(ordering);
            ParameterExpression parameter = Expression.Parameter(entityType);
            MemberExpression propertyAccess = Expression.MakeMemberAccess(parameter, property);
            LambdaExpression orderByExp = Expression.Lambda(propertyAccess, parameter);
            MethodCallExpression resultExp = Expression.Call(typeof(Queryable), "OrderBy", new Type[] { entityType, property.PropertyType }, source.Body, Expression.Quote(orderByExp));
            return Expression.Lambda<Func<DbContext, IQueryable<T>>>(resultExp, source.Parameters);
        }
        #endregion

        // Currently not used until issue from previous segment is fixed in EFCore
        #region DirectQuery
        /*public void UpdateOutputIdentity<T>(DbContext context, IList<T> entities) where T : class
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
                var entitiesWithOutputIdentity = await QueryOutputTable<T>(context).ToListAsync().ConfigureAwait(false);
                UpdateEntitiesIdentity(entities, entitiesWithOutputIdentity);
            }
        }

        protected IQueryable<T> QueryOutputTable<T>(DbContext context) where T : class
        {
            string q = SqlQueryBuilder.SelectFromOutputTable(this);
            var query = context.Set<T>().FromSql(q);
            if (!BulkConfig.TrackingEntities)
            {
                query = query.AsNoTracking();
            }

            var queryOrdered = OrderBy(query, PrimaryKeys[0]);
            // ALTERNATIVELY OrderBy with DynamicLinq ('using System.Linq.Dynamic.Core;' NuGet required) that eliminates need for custom OrderBy<T> method with Expression.
            //var queryOrdered = query.OrderBy(PrimaryKeys[0]);

            return queryOrdered;
        }

        private static IQueryable<T> OrderBy<T>(IQueryable<T> source, string ordering)
        {
            Type entityType = typeof(T);
            PropertyInfo property = entityType.GetProperty(ordering);
            ParameterExpression parameter = Expression.Parameter(entityType);
            MemberExpression propertyAccess = Expression.MakeMemberAccess(parameter, property);
            LambdaExpression orderByExp = Expression.Lambda(propertyAccess, parameter);
            MethodCallExpression resultExp = Expression.Call(typeof(Queryable), "OrderBy", new Type[] { entityType, property.PropertyType }, source.Expression, Expression.Quote(orderByExp));
            var orderedQuery = source.Provider.CreateQuery<T>(resultExp);
            return orderedQuery;
        }*/
        #endregion
    }
}
