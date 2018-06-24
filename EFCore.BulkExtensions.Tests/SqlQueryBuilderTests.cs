using System.Collections.Generic;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class SqlQueryBuilderUnitTests
    {
        [Fact]
        public void MergeTableInsertTest()
        {
            TableInfo tableInfo = GetTestTableInfo();
            tableInfo.HasIdentity = true;
            string result = SqlQueryBuilder.MergeTable(tableInfo, OperationType.Insert);

            string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T USING [dbo].[ItemTemp1234] AS S " +
                              "ON T.[ItemId] = S.[ItemId] " +
                              "WHEN NOT MATCHED THEN INSERT ([Name]) VALUES (S.[Name]);";

            Assert.Equal(result, expected);
        }

        [Fact]
        public void MergeTableInsertOrUpdateTest()
        {
            TableInfo tableInfo = GetTestTableInfo();
            tableInfo.HasIdentity = true;
            string result = SqlQueryBuilder.MergeTable(tableInfo, OperationType.InsertOrUpdate);

            string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T USING [dbo].[ItemTemp1234] AS S " +
                              "ON T.[ItemId] = S.[ItemId] " +
                              "WHEN NOT MATCHED THEN INSERT ([Name]) VALUES (S.[Name]) " +
                              "WHEN MATCHED THEN UPDATE SET T.[Name] = S.[Name];";

            Assert.Equal(result, expected);
        }

        [Fact]
        public void MergeTableUpdateTest()
        {
            TableInfo tableInfo = GetTestTableInfo();
            tableInfo.HasIdentity = true;
            string result = SqlQueryBuilder.MergeTable(tableInfo, OperationType.Update);

            string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T USING [dbo].[ItemTemp1234] AS S " +
                              "ON T.[ItemId] = S.[ItemId] " +
                              "WHEN MATCHED THEN UPDATE SET T.[Name] = S.[Name];";

            Assert.Equal(result, expected);
        }

        [Fact]
        public void MergeTableDeleteDeleteTest()
        {
            var tableInfo = GetTestTableInfo();
            string result = SqlQueryBuilder.MergeTable(tableInfo, OperationType.Delete);

            string expected = "MERGE [dbo].[Item] WITH (HOLDLOCK) AS T USING [dbo].[ItemTemp1234] AS S " +
                              "ON T.[ItemId] = S.[ItemId] " +
                              "WHEN MATCHED THEN DELETE;";

            Assert.Equal(result, expected);
        }

        private TableInfo GetTestTableInfo()
        {
            var tableInfo = new TableInfo()
            {
                Schema = "dbo",
                TableName = "Item",
                PrimaryKeys = new List<string> { "ItemId" },
                TempTableSufix = "Temp1234",
                BulkConfig = new BulkConfig()
            };
            var nameText = "Name";

            tableInfo.PropertyColumnNamesDict.Add(tableInfo.PrimaryKeys[0], tableInfo.PrimaryKeys[0]);
            tableInfo.PropertyColumnNamesDict.Add(nameText, nameText);

            return tableInfo;
        }
    }
}
