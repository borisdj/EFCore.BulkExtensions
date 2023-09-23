using EFCore.BulkExtensions.SqlAdapters;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;
using Xunit.Abstractions;

#nullable disable
namespace EFCore.BulkExtensions.Tests.BulkInsertOrUpdate;

public class EfCoreBulkInsertOrUpdateTests : IClassFixture<EfCoreBulkInsertOrUpdateTests.DatabaseFixture>
{
    private readonly ITestOutputHelper _writer;
    private readonly DatabaseFixture _dbFixture;

    public EfCoreBulkInsertOrUpdateTests(ITestOutputHelper writer, DatabaseFixture dbFixture)
    {
        _writer = writer;
        _dbFixture = dbFixture;
    }
    
    [Theory]
    [InlineData(SqlType.SqlServer)]
    public void BulkInsertOrUpdate_CustomUpdateBy_WithOutputIdentity_NullKeys(SqlType dbType)
    {
        using var db = _dbFixture.GetDb(dbType);
        
        var newItem = new SimpleItem()
        {
            StringProperty = null,
            GuidProperty = new Guid("ac53ed8d-5ee4-43cb-98c0-f12939949a49"),
        };

        var ensureList = new[] { newItem, };

        db.BulkInsertOrUpdate(ensureList,
            c =>
            {
                c.SetOutputIdentity = true;
                c.UpdateByProperties = new List<string> { nameof(SimpleItem.StringProperty) };
                c.PreserveInsertOrder = true;
            });

        var fromDb = db.SimpleItems.Single(x => x.GuidProperty == newItem.GuidProperty);

        Assert.NotNull(fromDb); // Item was inserted

        // Ids were correctly filled
        Assert.Equal(fromDb.Id, newItem.Id);
        Assert.NotEqual(0, newItem.Id);
    }
    
    public class DatabaseFixture : BulkDbTestsFixture
    {
        public DatabaseFixture() : base(nameof(EfCoreBulkInsertOrUpdateTests))
        {
        }
    }
}
