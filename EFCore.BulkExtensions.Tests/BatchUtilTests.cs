using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class BatchUtilTests
    {
        [Fact]
        public void GetBatchSql_UpdateSqlite_ReturnsExpectedValues()
        {
            ContextUtil.DbServer = DbServer.Sqlite;

            using (var context = new TestContext(ContextUtil.GetOptions()))
            {
                (string sql, string tableAlias, string tableAliasSufixAs, _, _)  = BatchUtil.GetBatchSql(context.Items, context, true);

                Assert.Equal("\"Item\"", tableAlias);
                Assert.Equal(" AS \"i\"", tableAliasSufixAs);
            }
        }
    }
}
