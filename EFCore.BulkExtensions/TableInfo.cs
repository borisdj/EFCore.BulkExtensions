using System;
using System.Collections.Generic;
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

        public string TempTableSufix { get; set; }
        public string FullTempTableName => $"{SchemaFormated}[{Name}{TempTableSufix}]";

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
    }
}
