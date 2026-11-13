using EFCore.BulkExtensions.SqlAdapters;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests.ShadowProperties;

public class ShadowKeyTests
{
    [Theory]
    [InlineData(SqlType.PostgreSql)]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    public void BulkInsert_EntityWithShadowPK_SavesToDatabase(SqlType dbServer)
    {
        var options = new ContextUtil(dbServer).GetOptions<ShadowKeyDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_ShadowProperties");
        using var db = new ShadowKeyDbContext(options);
        var data = new List<ShadowKeyModel>()
        {
            new ShadowKeyModel { Value = 42 }
        };

        db.BulkInsert(data);

        var modelFromDb = db.ShadowKeyModels.First();
        Assert.Equal(42, modelFromDb.Value);
        Assert.NotEqual(0, db.Entry(modelFromDb).Property(ShadowKeyModel.IdProperty).CurrentValue);
    }
}
