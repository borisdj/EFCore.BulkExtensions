using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests.ValueConverters;

public class ValueConverterTests
{
    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    [InlineData(SqlType.PostgreSql)]
    public void BulkInsertOrUpdate_EntityUsingBuiltInEnumToStringConverter_SavesToDatabase(SqlType sqlType)
    {
        using var db = new VcDbContext(DatabaseName, sqlType);

        db.BulkInsertOrUpdate(GetTestData().ToList());

        var connection = db.Database.GetDbConnection();
        if (connection.State != ConnectionState.Open)
        {
            connection.Open();
        }

        using var cmd = connection.CreateCommand();
        cmd.CommandText = GetSelectQuery(sqlType);
        cmd.CommandType = System.Data.CommandType.Text;

        using var reader = cmd.ExecuteReader();
        reader.Read();

        var enumStr = reader.Field<string>("Enum");

        Assert.Equal(VcEnum.Hello.ToString(), enumStr);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    [InlineData(SqlType.PostgreSql)]
    public void BatchUpdate_EntityUsingBuiltInEnumToStringConverter_UpdatesDatabaseWithEnumStringValue(SqlType sqlType)
    {
        using var db = new VcDbContext(DatabaseName, sqlType);

        db.BulkInsertOrUpdate(GetTestData().ToList());

        var date = new LocalDate(2020, 3, 21);
#pragma warning disable
        db.VcModels.Where(x => x.LocalDate > date).BatchUpdate(x => new VcModel
        {
            Enum = VcEnum.Why
        });

        var connection = db.Database.GetDbConnection();
        if (connection.State != ConnectionState.Open)
        {
            connection.Open();
        }

        using var cmd = connection.CreateCommand();
        cmd.CommandText = GetSelectQuery(sqlType);
        cmd.CommandType = System.Data.CommandType.Text;

        using var reader = cmd.ExecuteReader();
        reader.Read();

        var enumStr = reader.Field<string>("Enum");

        Assert.Equal(VcEnum.Why.ToString(), enumStr);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    [InlineData(SqlType.PostgreSql)]
    public void BatchDelete_UsingWhereExpressionWithValueConverter_Deletes(SqlType sqlType)
    {
        using var db = new VcDbContext(DatabaseName, sqlType);

        db.BulkInsertOrUpdate(GetTestData().ToList());

        var date = new LocalDate(2020, 3, 21);
        db.VcModels.Where(x => x.LocalDate > date).BatchDelete();

        var models = db.VcModels.Count();
        Assert.Equal(0, models);
    }

    private static IEnumerable<VcModel> GetTestData()
    {
        var one = new VcModel
        {
            Enum = VcEnum.Hello,
            LocalDate = new LocalDate(2021, 3, 22)
        };

        yield return one;
    }
    
    private static string GetSelectQuery(SqlType sqlType) =>
        sqlType == SqlType.PostgreSql
            ? "SELECT * FROM \"VcModels\" ORDER BY \"Id\" DESC"
            : "SELECT * FROM VcModels ORDER BY Id DESC";
    
    private static string DatabaseName => $"{nameof(EFCoreBulkTest)}_ValueConverters";
}
