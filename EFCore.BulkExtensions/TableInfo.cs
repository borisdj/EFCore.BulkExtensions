using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
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
            var conn = context.Database.GetDbConnection();
            try
            {
                conn.OpenAsync();
                using (var command = conn.CreateCommand())
                {
                    string query = SqlQueryBuilder.SelectIsIdentity(FullTableName, PrimaryKey);
                    command.CommandText = query;
                    DbDataReader reader = command.ExecuteReader();

                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            hasIdentity = (int)reader[0];
                        }
                    }
                    reader.Dispose();
                }
            }
            finally
            {
                conn.Close();
            }

            HasIdentity = hasIdentity == 1;
        }
    }
}
