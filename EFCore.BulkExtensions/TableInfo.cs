using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using FastMember;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    public class TableInfo
    {
        public string Schema { get; set; }
        public string SchemaFormated => Schema != null ? Schema + "." : "";
        public string Name { get; set; }
        public string FullTableName => $"{SchemaFormated}[{Name}]";
        public string PrimaryKey { get; set; }
        public string PrimaryKeyFormated => $"[{PrimaryKey}]";

        public string TempTableSufix { get; set; }
        public string FullTempTableName => $"{SchemaFormated}[{Name}{TempTableSufix}]";
        public string FullTempOutputTableName => $"{SchemaFormated}[{Name}{TempTableSufix}Output]";

        public bool InsertToTempTable { get; set; }
        public bool HasIdentity { get; set; }
        public int NumberOfEntities { get; set; }
        public BulkConfig BulkConfig { get; set; }
        public Dictionary<string, string> PropertyColumnNamesDict = new Dictionary<string, string>();

        public static TableInfo CreateInstance<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig)
        {
            var tableInfo = new TableInfo();
            var isDeleteOperation = operationType == OperationType.Delete;
            tableInfo.NumberOfEntities = entities.Count;
            tableInfo.LoadData<T>(context, isDeleteOperation);
            tableInfo.BulkConfig = bulkConfig ?? new BulkConfig();
            return tableInfo;
        }

        public void LoadData<T>(DbContext context, bool loadOnlyPKColumn)
        {
            var entityType = context.Model.FindEntityType(typeof(T));
            if (entityType == null)
                throw new InvalidOperationException("DbContext does not contain EntitySet for Type: " + typeof(T).Name);

            var relationalData = entityType.Relational();
            Schema = relationalData.Schema;
            Name = relationalData.TableName;
            TempTableSufix = Guid.NewGuid().ToString().Substring(0, 8); // 8 chars of Guid as tableNameSufix to avoid same name collision with other tables

            PrimaryKey = entityType.FindPrimaryKey().Properties.First().Name;

            if (loadOnlyPKColumn)
            {
                PropertyColumnNamesDict.Add(PrimaryKey, PrimaryKey);
            }
            else
            {
                PropertyColumnNamesDict = entityType.GetProperties().ToDictionary(a => a.Name, b => b.Relational().ColumnName);
            }
        }

        public void CheckHasIdentity(DbContext context)
        {
            int hasIdentity = 0;
            var connection = context.Database.GetDbConnection();
            try
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = SqlQueryBuilder.SelectIsIdentity(FullTableName, PrimaryKey);;
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
                connection.Close();
            }
            HasIdentity = hasIdentity == 1;
        }

        public async Task CheckHasIdentityAsync(DbContext context)
        {
            int hasIdentity = 0;
            var connection = context.Database.GetDbConnection();
            try
            {
                connection.Open();
                using (var command = connection.CreateCommand())
                {
                    command.CommandText = SqlQueryBuilder.SelectIsIdentity(FullTableName, PrimaryKey);
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        if (reader.HasRows)
                        {
                            while (await reader.ReadAsync())
                            {
                                hasIdentity = (int)reader[0];
                            }
                        }
                    }
                }
            }
            finally
            {
                connection.Close();
            }
            HasIdentity = hasIdentity == 1;
        }

        public void SetSqlBulkCopyConfig<T>(SqlBulkCopy sqlBulkCopy, IList<T> entities, Action<double> progress)
        {
            sqlBulkCopy.DestinationTableName = this.InsertToTempTable ? this.FullTempTableName : this.FullTableName;
            sqlBulkCopy.BatchSize = BulkConfig.BatchSize;
            sqlBulkCopy.NotifyAfter = BulkConfig.BatchSize;
            sqlBulkCopy.SqlRowsCopied += (sender, e) => { progress?.Invoke(e.RowsCopied / entities.Count); };

            foreach (var element in this.PropertyColumnNamesDict)
            {
                sqlBulkCopy.ColumnMappings.Add(element.Key, element.Value);
            }
        }

        public void UpdateOutputIdentity<T>(DbContext context, IList<T> entities) where T : class
        {
            var entitiesWithOutputIdentity = context.Set<T>().FromSql(SqlQueryBuilder.SelectFromTable(this.FullTempOutputTableName, this.PrimaryKeyFormated)).ToList();
            if (this.BulkConfig.PreserveInsertOrder) // Updates PK in entityList
            {
                var accessor = TypeAccessor.Create(typeof(T));
                for (int i = 0; i < this.NumberOfEntities; i++)
                    accessor[entities[i], this.PrimaryKey] = accessor[entitiesWithOutputIdentity[i], this.PrimaryKey];
            }
            else // Clears entityList and then refill it with loaded entites from Db
            {
                entities.Clear();
                ((List<T>)entities).AddRange(entitiesWithOutputIdentity);
            }
        }
    }
}
