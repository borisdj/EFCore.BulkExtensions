using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.SqlAdapters.GaussDB;
using GaussDB;
using GaussDBTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests.GaussDB;

public class GaussDBSqlQueryBuilderTests
{
    [Fact]
    public void DbServer_ExposesGaussDBProviderParts()
    {
        var server = new GaussDBDbServer();

        Assert.Equal(SqlType.GaussDB, server.Type);
        Assert.IsType<GaussDBAdapter>(server.Adapter);
        Assert.IsType<GaussDBDialect>(server.Dialect);
        Assert.IsType<GaussDBQueryBuilder>(server.QueryBuilder);
    }

    [Fact]
    public void QueryBuilder_CreatesGaussDBCommandAndParameter()
    {
        var builder = new GaussDBQueryBuilder();

        Assert.IsType<GaussDBCommand>(builder.CreateCommand());

        var parameter = Assert.IsType<GaussDBParameter>(builder.CreateParameter("@Payload", "{}"));
        Assert.Equal("@Payload", parameter.ParameterName);
        Assert.Equal("{}", parameter.Value);

        builder.SetDbTypeParam(parameter, builder.Dbtype());
        Assert.Equal(GaussDBDbType.Jsonb, parameter.GaussDBDbType);
    }

    [Fact]
    public void MergeTable_InsertOrUpdateWithoutWhere_GeneratesGaussDBUpsert()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.IdentityColumnName = "ItemId";

        string actual = GaussDBQueryBuilder.MergeTable<GaussDBSqlItem>(tableInfo, OperationType.InsertOrUpdate);

        string expected = @"INSERT INTO ""dbo"".""GaussDBSqlItem"" (""ItemId"", ""Name"") " +
                          @"(SELECT ""ItemId"", ""Name"" FROM ""dbo"".""GaussDBSqlItemTemp1234"") " +
                          @"ON DUPLICATE KEY UPDATE ""Name"" = EXCLUDED.""Name"";";
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void MergeTable_InsertOrUpdateWithWhere_ThrowsNotSupported()
    {
        TableInfo tableInfo = GetTestTableInfo((existing, inserted) => $"{inserted}.ItemTimestamp > {existing}.ItemTimestamp");
        tableInfo.IdentityColumnName = "ItemId";

        var exception = Assert.Throws<NotSupportedException>(() =>
            GaussDBQueryBuilder.MergeTable<GaussDBSqlItem>(tableInfo, OperationType.InsertOrUpdate));

        Assert.Contains(nameof(BulkConfig.OnConflictUpdateWhereSql), exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void MergeTable_InsertOnly_GeneratesDoNothing()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.IdentityColumnName = "ItemId";
        tableInfo.PropertyColumnNamesUpdateDict = new();
        tableInfo.BulkConfig.ApplySubqueryLimit = 1;

        string actual = GaussDBQueryBuilder.MergeTable<GaussDBSqlItem>(tableInfo, OperationType.InsertOrUpdate);

        string expected = @"INSERT INTO ""dbo"".""GaussDBSqlItem"" (""ItemId"", ""Name"") " +
                          @"(SELECT ""ItemId"", ""Name"" FROM ""dbo"".""GaussDBSqlItemTemp1234"") LIMIT 1 " +
                          @"ON DUPLICATE KEY UPDATE ""Name"" = ""dbo"".""GaussDBSqlItem"".""Name"";";
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void MergeTable_Update_GeneratesUpdateFromTempTable()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.IdentityColumnName = "ItemId";

        string actual = GaussDBQueryBuilder.MergeTable<GaussDBSqlItem>(tableInfo, OperationType.Update);

        string expected = @"UPDATE ""dbo"".""GaussDBSqlItem"" SET ""Name"" = ""dbo"".""GaussDBSqlItemTemp1234"".""Name"" " +
                          @"FROM ""dbo"".""GaussDBSqlItemTemp1234"" " +
                          @"WHERE ""dbo"".""GaussDBSqlItem"".""ItemId"" = ""dbo"".""GaussDBSqlItemTemp1234"".""ItemId"";";
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void MergeTable_Read_GeneratesJoinOnPrimaryKey()
    {
        TableInfo tableInfo = GetTestTableInfo();

        string actual = GaussDBQueryBuilder.MergeTable<GaussDBSqlItem>(tableInfo, OperationType.Read);

        string expected = @"SELECT ""dbo"".""GaussDBSqlItem"".* FROM ""dbo"".""GaussDBSqlItem"" " +
                          @"JOIN ""dbo"".""GaussDBSqlItemTemp1234"" " +
                          @"USING (""ItemId"");";
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void MergeTable_Delete_GeneratesDeleteUsingTempTable()
    {
        TableInfo tableInfo = GetTestTableInfo();

        string actual = GaussDBQueryBuilder.MergeTable<GaussDBSqlItem>(tableInfo, OperationType.Delete);

        string expected = @"DELETE FROM ""dbo"".""GaussDBSqlItem"" " +
                          @"USING ""dbo"".""GaussDBSqlItemTemp1234"" " +
                          @"WHERE ""dbo"".""GaussDBSqlItem"".""ItemId"" = ""dbo"".""GaussDBSqlItemTemp1234"".""ItemId"";";
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void MergeTable_InsertOrUpdateOrDelete_ThrowsNotSupported()
    {
        TableInfo tableInfo = GetTestTableInfo();

        var exception = Assert.Throws<NotImplementedException>(() =>
            GaussDBQueryBuilder.MergeTable<GaussDBSqlItem>(tableInfo, OperationType.InsertOrUpdateOrDelete));

        Assert.Contains("GaussDB", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void InsertIntoTable_UsesCopyWithBinaryFormat()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.IdentityColumnName = "ItemId";
        tableInfo.InsertToTempTable = true;

        string actual = GaussDBQueryBuilder.InsertIntoTable(tableInfo, OperationType.InsertOrUpdate);

        string expected = @"COPY ""dbo"".""GaussDBSqlItemTemp1234"" (""ItemId"", ""Name"") FROM STDIN (FORMAT BINARY);";
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void InsertIntoTable_OmitsIdentityForDirectInsert()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.IdentityColumnName = "ItemId";

        string actual = GaussDBQueryBuilder.InsertIntoTable(tableInfo, OperationType.Insert);

        string expected = @"COPY ""dbo"".""GaussDBSqlItem"" (""Name"") FROM STDIN (FORMAT BINARY);";
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void DdlHelpers_GenerateGaussDBSql()
    {
        TableInfo tableInfo = GetTestTableInfo();

        Assert.Equal(@"TRUNCATE ""dbo"".""GaussDBSqlItem"";",
            new GaussDBQueryBuilder().TruncateTable(tableInfo.FullTableName));
        Assert.Equal(@"DROP TABLE IF EXISTS ""dbo"".""GaussDBSqlItemTemp1234""",
            GaussDBQueryBuilder.DropTable(tableInfo.FullTempTableName));
        Assert.Equal(@"CREATE TABLE ""dbo"".""GaussDBSqlItemTemp1234"" AS TABLE ""dbo"".""GaussDBSqlItem"" WITH NO DATA;",
            GaussDBQueryBuilder.CreateTableCopy(tableInfo.FullTableName, tableInfo.FullTempTableName, useTempDb: false, unlogged: false));
        Assert.Equal(@"CREATE UNLOGGED TABLE IF NOT EXISTS ""dbo"".""GaussDBSqlItemTemp1234Output"" (""xmaxNumber"" xid)",
            GaussDBQueryBuilder.CreateOutputStatsTable(tableInfo.FullTempOutputTableName, useTempDb: false, unlogged: true));
        Assert.Equal(@"CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ""tempUniqueIndex_dbo_GaussDBSqlItem_ItemId"" ON ""dbo"".""GaussDBSqlItem"" (""ItemId"")",
            GaussDBQueryBuilder.CreateUniqueIndex(tableInfo));
        Assert.Equal(@"ALTER TABLE ""dbo"".""GaussDBSqlItem"" ADD CONSTRAINT ""tempUniqueIndex_dbo_GaussDBSqlItem_ItemId"" UNIQUE USING INDEX ""tempUniqueIndex_dbo_GaussDBSqlItem_ItemId""",
            GaussDBQueryBuilder.CreateUniqueConstrain(tableInfo));
        Assert.Equal(@"DROP INDEX ""dbo"".""tempUniqueIndex_dbo_GaussDBSqlItem_ItemId"";",
            GaussDBQueryBuilder.DropUniqueIndex(tableInfo));
        Assert.Equal(@"ALTER TABLE ""dbo"".""GaussDBSqlItem"" DROP CONSTRAINT ""tempUniqueIndex_dbo_GaussDBSqlItem_ItemId"";",
            GaussDBQueryBuilder.DropUniqueConstrain(tableInfo));
    }

    [Fact]
    public void CountUniqueConstraintAndIndex_IncludeTargetTableAndColumns()
    {
        TableInfo tableInfo = GetTestTableInfo();

        string constraintSql = GaussDBQueryBuilder.CountUniqueConstrain(tableInfo);
        string indexSql = GaussDBQueryBuilder.CountUniqueIndex(tableInfo);

        Assert.Contains("pg_catalog.pg_constraint", constraintSql, StringComparison.Ordinal);
        Assert.Contains("r.relname = 'GaussDBSqlItem'", constraintSql, StringComparison.Ordinal);
        Assert.Contains("nr.nspname = 'dbo'", constraintSql, StringComparison.Ordinal);
        Assert.Contains("a.attname IN('ItemId')", constraintSql, StringComparison.Ordinal);

        Assert.Contains("pg_catalog.pg_index", indexSql, StringComparison.Ordinal);
        Assert.Contains("tbl.relname = 'GaussDBSqlItem'", indexSql, StringComparison.Ordinal);
        Assert.Contains("tnsp.nspname = 'dbo'", indexSql, StringComparison.Ordinal);
        Assert.Contains("at.attname IN('ItemId')", indexSql, StringComparison.Ordinal);
    }

    [Fact]
    public void RestructureForBatchWithoutJoin_GeneratesGaussDBUpdate()
    {
        string sql =
            @"UPDATE i SET ""Description"" = @Description, ""Price"" = @Price FROM ""Item"" AS i WHERE i.""ItemId"" <= 1";

        string expected =
            @"UPDATE ""Item"" AS i SET ""Description"" = @Description, ""Price"" = @Price WHERE i.""ItemId"" <= 1";

        string actual = new GaussDBQueryBuilder().RestructureForBatch(sql);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void RestructureForBatchWithJoin_GeneratesGaussDBUpdateWithFromJoinTarget()
    {
        string sql =
            @"UPDATE i SET ""Description"" = @Description, ""Price"" = @Price FROM ""Item"" AS i INNER JOIN ""User"" AS u ON i.""UserId"" = u.""Id"" WHERE i.""ItemId"" <= 1";

        string expected =
            @"UPDATE ""Item"" AS i SET ""Description"" = @Description, ""Price"" = @Price FROM ""User"" AS u WHERE i.""ItemId"" <= 1 AND i.""UserId"" = u.""Id"" ";

        string actual = new GaussDBQueryBuilder().RestructureForBatch(sql);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void RestructureForBatchDelete_GeneratesGaussDBDelete()
    {
        string sql = @"DELETE i FROM ""Item"" AS i WHERE i.""ItemId"" <= 1";

        string actual = new GaussDBQueryBuilder().RestructureForBatch(sql, isDelete: true);

        Assert.Equal(@"DELETE FROM ""Item"" AS i WHERE i.""ItemId"" <= 1", actual);
    }

    [Fact]
    public void GetUniqueIndexName_TruncatesLongNames()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.TableName = "Temp1234567891011121314151617181920212223";

        string actual = GaussDBQueryBuilder.GetUniqueIndexName(tableInfo);

        Assert.Equal("tempUniqueIndex_dbo_Temp1234567891011121314151617181920212223_It", actual);
    }

    private static TableInfo GetTestTableInfo(Func<string, string, string>? onConflictUpdateWhereSql = null)
    {
        var tableInfo = new TableInfo
        {
            Schema = "dbo",
            TempSchema = "dbo",
            TableName = nameof(GaussDBSqlItem),
            TempTableName = nameof(GaussDBSqlItem) + "Temp1234",
            TempTableSufix = "Temp1234",
            PrimaryKeysPropertyColumnNameDict = new Dictionary<string, string>
            {
                { nameof(GaussDBSqlItem.ItemId), nameof(GaussDBSqlItem.ItemId) }
            },
            EntityPKPropertyColumnNameDict = new Dictionary<string, string>
            {
                { nameof(GaussDBSqlItem.ItemId), nameof(GaussDBSqlItem.ItemId) }
            },
            BulkConfig = new BulkConfig
            {
                OnConflictUpdateWhereSql = onConflictUpdateWhereSql
            }
        };

        tableInfo.PropertyColumnNamesDict.Add(
            tableInfo.PrimaryKeysPropertyColumnNameDict.Keys.First(),
            tableInfo.PrimaryKeysPropertyColumnNameDict.Values.First());
        tableInfo.PropertyColumnNamesDict.Add(nameof(GaussDBSqlItem.Name), nameof(GaussDBSqlItem.Name));
        tableInfo.PropertyColumnNamesCompareDict = tableInfo.PropertyColumnNamesDict;
        tableInfo.PropertyColumnNamesUpdateDict = tableInfo.PropertyColumnNamesDict;
        tableInfo.OutputPropertyColumnNamesDict = tableInfo.PropertyColumnNamesDict;
        return tableInfo;
    }

    private class GaussDBSqlItem
    {
        public int ItemId { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
