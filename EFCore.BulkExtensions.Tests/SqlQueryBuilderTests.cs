using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class SqlQueryBuilderUnitTests
    {
        [Fact]
        public void MergeTableUpdateTest()
        {
            var tableInfo = GetTestTableInfo();
            string result = SqlQueryBuilder.MergeTable(tableInfo, OperationType.Update);

            string expectedResult = "MERGE dbo.Item WITH (HOLDLOCK) USING dbo.ItemTemp1234 " +
                                    "ON dbo.Item.ItemId = dbo.ItemTemp1234.ItemId " +
                                    "WHEN MATCHED THEN UPDATE SET dbo.Item.Description = dbo.ItemTemp1234.Description;";

            Assert.Equal(result, expectedResult);
        }

        [Fact]
        public void MergeTableInsertOrUpdateTest()
        {
            var tableInfo = GetTestTableInfo();
            string result = SqlQueryBuilder.MergeTable(tableInfo, OperationType.InsertOrUpdate);

            string expectedResult = "MERGE dbo.Item WITH (HOLDLOCK) USING dbo.ItemTemp1234 " +
                                    "ON dbo.Item.ItemId = dbo.ItemTemp1234.ItemId " +
                                    "WHEN MATCHED THEN UPDATE SET dbo.Item.Description = dbo.ItemTemp1234.Description " +
                                    "WHEN NOT MATCHED THEN INSERT (ItemId,Description) VALUES (dbo.ItemTemp1234.ItemId,dbo.ItemTemp1234.Description);";
            
            Assert.Equal(result, expectedResult);
        }

        [Fact]
        public void MergeTableDeleteDeleteTest()
        {
            var tableInfo = GetTestTableInfo();
            string result = SqlQueryBuilder.MergeTable(tableInfo, OperationType.Delete);

            string expectedResult = "MERGE dbo.Item WITH (HOLDLOCK) USING dbo.ItemTemp1234 " +
                                    "ON dbo.Item.ItemId = dbo.ItemTemp1234.ItemId " +
                                    "WHEN MATCHED THEN DELETE;";

            Assert.Equal(result, expectedResult);
        }

        private TableInfo GetTestTableInfo()
        {
            var tableInfo = new TableInfo()
            {
                Schema = "dbo",
                Name = "Item",
                PrimaryKey = "ItemId",
                TempTableSufix = "Temp1234"
            };
            var descriptionText = "Description";

            tableInfo.PropertyColumnNamesDict.Add(tableInfo.PrimaryKey, tableInfo.PrimaryKey);
            tableInfo.PropertyColumnNamesDict.Add(descriptionText, descriptionText);

            return tableInfo;
        }
    }
}
