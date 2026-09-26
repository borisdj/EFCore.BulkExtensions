using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.SqlAdapters.GaussDB;
using GaussDB;
using GaussDB.EntityFrameworkCore.PostgreSQL.Metadata;
using GaussDBTypes;
using Microsoft.EntityFrameworkCore.Infrastructure;
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

    [Theory]
    [InlineData(GaussDBValueGenerationStrategy.IdentityByDefaultColumn, true)]
    [InlineData(GaussDBValueGenerationStrategy.IdentityAlwaysColumn, true)]
    [InlineData(GaussDBValueGenerationStrategy.SerialColumn, true)]
    [InlineData(GaussDBValueGenerationStrategy.SequenceHiLo, false)]
    [InlineData(GaussDBValueGenerationStrategy.None, false)]
    public void DbServer_IdentifiesDatabaseGeneratedIdentityStrategies(GaussDBValueGenerationStrategy strategy, bool expected)
    {
        var server = new GaussDBDbServer();

        Assert.Equal(expected, server.PropertyHasIdentity(new Annotation(server.ValueGenerationStrategy, strategy)));
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
    public void SelectFromOutputTable_UsesGaussDBIdentifiers()
    {
        var tableInfo = GetTestTableInfo();
        var actual = new GaussDBQueryBuilder().SelectFromOutputTable(tableInfo);

        Assert.Equal(@"SELECT ""ItemId"", ""Name"" FROM ""dbo"".""GaussDBSqlItemTemp1234Output"" WHERE ""ItemId"" IS NOT NULL", actual);
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
    public void MergeTable_Read_WithNullableKey_UsesNullSafeJoin()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.UpdateByPropertiesAreNullable = true;

        string actual = GaussDBQueryBuilder.MergeTable<GaussDBSqlItem>(tableInfo, OperationType.Read);

        Assert.Contains(" ON ", actual, StringComparison.Ordinal);
        Assert.Contains("IS NULL", actual, StringComparison.Ordinal);
        Assert.DoesNotContain("USING (", actual, StringComparison.Ordinal);
    }

    [Fact]
    public void MergeTable_Update_IncludesDefaultValuePropertyWithoutUpdatingKey()
    {
        TableInfo tableInfo = GetTestTableInfo();
        tableInfo.DefaultValueProperties = new HashSet<string> { nameof(GaussDBSqlItem.Name) };

        string actual = GaussDBQueryBuilder.MergeTable<GaussDBSqlItem>(tableInfo, OperationType.Update);

        Assert.Contains(" SET \"Name\" = ", actual, StringComparison.Ordinal);
        Assert.DoesNotContain("SET \"ItemId\"", actual, StringComparison.Ordinal);
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

    [Theory]
    [InlineData(OperationType.Insert)]
    [InlineData(OperationType.InsertOrUpdate)]
    [InlineData(OperationType.Update)]
    [InlineData(OperationType.Delete)]
    [InlineData(OperationType.Read)]
    public void MergeTable_CustomSourceMappings_ProjectSourceColumnsForEveryOperation(OperationType operation)
    {
        var tableInfo = GetTestTableInfo();
        tableInfo.TempSchema = "stage";
        tableInfo.TempTableName = "ImportItems";
        tableInfo.BulkConfig.CustomSourceTableName = "stage.ImportItems";
        tableInfo.BulkConfig.CustomSourceDestinationMappingColumns = new()
        {
            ["SourceId"] = "ItemId",
            ["SourceName"] = "Name",
        };

        string actual = GaussDBQueryBuilder.MergeTable<GaussDBSqlItem>(tableInfo, operation);

        var projectedColumns = operation is OperationType.Read or OperationType.Delete
            ? "\"SourceId\" AS \"ItemId\""
            : "\"SourceId\" AS \"ItemId\", \"SourceName\" AS \"Name\"";
        Assert.Contains($"(SELECT {projectedColumns} FROM \"stage\".\"ImportItems\") AS \"__bulk_source\"", actual, StringComparison.Ordinal);
        if (operation is OperationType.Update or OperationType.Delete)
        {
            Assert.Contains("\"dbo\".\"GaussDBSqlItem\".\"ItemId\" = \"__bulk_source\".\"ItemId\"", actual, StringComparison.Ordinal);
        }
        if (operation == OperationType.Update)
        {
            Assert.Contains("SET \"Name\" = \"__bulk_source\".\"Name\"", actual, StringComparison.Ordinal);
        }
        if (operation == OperationType.Read)
        {
            Assert.Contains("SELECT \"dbo\".\"GaussDBSqlItem\".*", actual, StringComparison.Ordinal);
            Assert.DoesNotContain("SourceName", actual, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void MergeTable_CustomSourceMappings_WithNullableReadKeyUseProjectedJoinColumn()
    {
        var tableInfo = GetTestTableInfo();
        tableInfo.BulkConfig.CustomSourceTableName = tableInfo.TempTableName;
        tableInfo.BulkConfig.CustomSourceDestinationMappingColumns = new() { ["SourceId"] = "ItemId" };
        tableInfo.UpdateByPropertiesAreNullable = true;

        string actual = GaussDBQueryBuilder.MergeTable<GaussDBSqlItem>(tableInfo, OperationType.Read);

        Assert.Contains("SELECT \"SourceId\" AS \"ItemId\" FROM", actual, StringComparison.Ordinal);
        Assert.Contains("\"__bulk_source\".\"ItemId\" IS NULL", actual, StringComparison.Ordinal);
        Assert.DoesNotContain("SELECT \"SourceId\".*", actual, StringComparison.Ordinal);
    }

    [Fact]
    public void MergeTable_CustomSourceMappings_DoNotCascadeColumnNameReplacements()
    {
        var tableInfo = GetTestTableInfo();
        tableInfo.BulkConfig.CustomSourceTableName = tableInfo.TempTableName;
        tableInfo.BulkConfig.CustomSourceDestinationMappingColumns = new()
        {
            ["Name"] = "ItemId",
            ["ItemId"] = "Name",
        };

        string actual = GaussDBQueryBuilder.MergeTable<GaussDBSqlItem>(tableInfo, OperationType.Update);

        Assert.Contains("SELECT \"Name\" AS \"ItemId\", \"ItemId\" AS \"Name\" FROM", actual, StringComparison.Ordinal);
        Assert.Contains("SET \"Name\" = \"__bulk_source\".\"Name\"", actual, StringComparison.Ordinal);
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
        Assert.Equal(@"CREATE UNIQUE INDEX IF NOT EXISTS ""tempUniqueIndex_dbo_GaussDBSqlItem_ItemId"" ON ""dbo"".""GaussDBSqlItem"" (""ItemId"")",
            GaussDBQueryBuilder.CreateUniqueIndex(tableInfo, concurrently: false));
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
    public void RestructureForBatchUpdateWithoutWhere_DoesNotThrow()
    {
        const string sql = @"UPDATE item_alias SET ""Name"" = @Name FROM ""Item"" AS item_alias";

        var actual = new GaussDBQueryBuilder().RestructureForBatch(sql);

        Assert.Equal(@"UPDATE ""Item"" AS item_alias SET ""Name"" = @Name", actual);
    }

    [Fact]
    public void RestructureForBatchWithLongAlias_PreservesAlias()
    {
        const string sql = @"UPDATE item_alias SET ""Name"" = @Name FROM ""Item"" AS item_alias WHERE item_alias.""ItemId"" = 1";

        var actual = new GaussDBQueryBuilder().RestructureForBatch(sql);

        Assert.Equal(@"UPDATE ""Item"" AS item_alias SET ""Name"" = @Name WHERE item_alias.""ItemId"" = 1", actual);
    }

    [Theory]
    [InlineData("i")]
    [InlineData("item12")]
    [InlineData("\"item12\"")]
    public void RestructureForBatchUpdate_WithoutWherePreservesCompleteAlias(string alias)
    {
        string sql = $"UPDATE {alias} SET \"Quantity\" = {alias}.\"Quantity\" + @delta FROM \"Item\" AS {alias}";

        string actual = new GaussDBQueryBuilder().RestructureForBatch(sql);

        Assert.Equal($"UPDATE \"Item\" AS {alias} SET \"Quantity\" = {alias}.\"Quantity\" + @delta", actual);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RestructureForBatch_WithTagWithPreservesCommentAndCompleteAlias(bool isDelete)
    {
        const string comments = "-- UPDATE FROM is a query tag\n\n/* batch operation */\n";
        string sql = comments + (isDelete
            ? "DELETE item12 FROM \"Item\" AS item12 WHERE item12.\"ItemId\" = @id"
            : "UPDATE item12 SET \"Name\" = @name FROM \"Item\" AS item12 WHERE item12.\"ItemId\" = @id");

        string actual = new GaussDBQueryBuilder().RestructureForBatch(sql, isDelete);

        Assert.Equal(comments + (isDelete
            ? "DELETE FROM \"Item\" AS item12 WHERE item12.\"ItemId\" = @id"
            : "UPDATE \"Item\" AS item12 SET \"Name\" = @name WHERE item12.\"ItemId\" = @id"), actual);
    }

    [Fact]
    public void RestructureForBatchUpdate_WithJoinWithoutWhereKeepsJoinPredicate()
    {
        const string sql = "UPDATE item12 SET \"Name\" = @name FROM \"Item\" AS item12 " +
            "INNER JOIN \"User\" AS user12 ON item12.\"UserId\" = user12.\"Id\"";

        string actual = new GaussDBQueryBuilder().RestructureForBatch(sql);

        Assert.Equal("UPDATE \"Item\" AS item12 SET \"Name\" = @name FROM \"User\" AS user12 " +
            "WHERE item12.\"UserId\" = user12.\"Id\"", actual);
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
