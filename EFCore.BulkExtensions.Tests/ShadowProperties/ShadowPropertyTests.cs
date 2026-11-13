using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests.ShadowProperties;

public class ShadowPropertyTests
{
    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    public void BulkInsertOrUpdate_EntityWithShadowProperties_SavesToDatabase(SqlType dbServer)
    {
        var options = new ContextUtil(dbServer).GetOptions<SpDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_ShadowProperties");
        using var db = new SpDbContext(options);

        db.BulkInsertOrUpdate(GetTestData(db, true, 10000).ToList(), new BulkConfig
        {
            EnableShadowProperties = true
        });

        var modelFromDb = db.SpModels.OrderByDescending(y => y.Id).First();
        Assert.Equal((long)10, db.Entry(modelFromDb).Property(SpModel.SpLong).CurrentValue);
        Assert.Null(db.Entry(modelFromDb).Property(SpModel.SpNullableLong).CurrentValue);

        Assert.Equal(new DateTime(2021, 02, 14), db.Entry(modelFromDb).Property(SpModel.SpDateTime).CurrentValue);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    public void BulkInsertOrUpdate_EntityWithShadowProperties_GlobalFunc_SavesToDatabase(SqlType dbServer)
    {
        var options = new ContextUtil(dbServer)
            .GetOptions<SpDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_ShadowProperties");
        
        using var db = new SpDbContext(options);

        var data = GetTestData(db, false, 10000);

        db.BulkInsertOrUpdate(data.ToList(), new BulkConfig
        {
            EnableShadowProperties = true,
            ShadowPropertyValue = (entity, property) =>
            {

                if (property == SpModel.SpLong)
                {
                    return 10;
                }
                else if (property == SpModel.SpNullableLong)
                {
                    return null;
                }
                else if (property == SpModel.SpDateTime)
                {
                    return new DateTime(2021, 02, 14);
                }

                return property;

            }
        });

        var modelFromDb = db.SpModels.OrderByDescending(y => y.Id).First();
        Assert.Equal((long)10, db.Entry(modelFromDb).Property(SpModel.SpLong).CurrentValue);
        Assert.Null(db.Entry(modelFromDb).Property(SpModel.SpNullableLong).CurrentValue);

        Assert.Equal(new DateTime(2021, 02, 14), db.Entry(modelFromDb).Property(SpModel.SpDateTime).CurrentValue);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    public void BulkInsert_EntityWithShadowProperties_WithoutEntry_UsesShadowPropertyValue(SqlType dbServer)
    {
        var options = new ContextUtil(dbServer)
            .GetOptions<SpDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_ShadowProperties_WithoutEntry");

        using var db = new SpDbContext(options);
        var entity = new SpModel();
        var callbackCalled = false;

        var exception = Record.Exception(() => db.BulkInsert(new[] { entity }, new BulkConfig
        {
            EnableShadowProperties = true,
            ShadowPropertyValue = (instance, property) =>
            {
                callbackCalled = true;
                Assert.Same(entity, instance);

                if (property == SpModel.SpLong)
                {
                    return 42L;
                }

                if (property == SpModel.SpNullableLong)
                {
                    return null;
                }

                if (property == SpModel.SpDateTime)
                {
                    return new DateTime(2022, 01, 01);
                }

                throw new InvalidOperationException($"Unexpected shadow property '{property}'.");
            }
        }));

        Assert.Null(exception);
        Assert.True(callbackCalled);

        var modelFromDb = db.SpModels.Single();
        Assert.Equal(42L, db.Entry(modelFromDb).Property(SpModel.SpLong).CurrentValue);
        Assert.Null(db.Entry(modelFromDb).Property(SpModel.SpNullableLong).CurrentValue);
        Assert.Equal(new DateTime(2022, 01, 01), db.Entry(modelFromDb).Property(SpModel.SpDateTime).CurrentValue);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    public void BulkInsertOrUpdate_EntityWithShadowProperties_PropertiesToExcludeOnUpdate_UsesShadowPropertyValueOnCreate(SqlType dbServer)
    {
        var options = new ContextUtil(dbServer)
            .GetOptions<SpDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_ShadowProperties_ExcludeOnUpdate_Create");

        using var db = new SpDbContext(options);
        var entity = new SpModel();
        var callbackCalled = false;

        var exception = Record.Exception(() => db.BulkInsertOrUpdate(new[] { entity }, new BulkConfig
        {
            EnableShadowProperties = true,
            PropertiesToExcludeOnUpdate = new List<string> { SpModel.SpLong },
            ShadowPropertyValue = (instance, property) =>
            {
                callbackCalled = true;
                Assert.Same(entity, instance);

                if (property == SpModel.SpLong)
                {
                    return 42L;
                }

                if (property == SpModel.SpNullableLong)
                {
                    return null;
                }

                if (property == SpModel.SpDateTime)
                {
                    return new DateTime(2022, 01, 01);
                }

                throw new InvalidOperationException($"Unexpected shadow property '{property}'.");
            }
        }));

        Assert.Null(exception);
        Assert.True(callbackCalled);

        var modelFromDb = db.SpModels.Single();
        Assert.Equal(42L, db.Entry(modelFromDb).Property(SpModel.SpLong).CurrentValue);
        Assert.Null(db.Entry(modelFromDb).Property(SpModel.SpNullableLong).CurrentValue);
        Assert.Equal(new DateTime(2022, 01, 01), db.Entry(modelFromDb).Property(SpModel.SpDateTime).CurrentValue);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    public void BulkInsertOrUpdate_EntityWithShadowProperties_PropertiesToExclude_DoesNotThrow(SqlType dbServer)
    {
        var options = new ContextUtil(dbServer)
            .GetOptions<SpDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_ShadowProperties_Exclude");
 
        using var db = new SpDbContext(options);
        var data = GetTestData(db, true, 10).ToList();
 
        var exception = Record.Exception(() => db.BulkInsertOrUpdate(data, new BulkConfig
        {
            EnableShadowProperties = true,
            PropertiesToExclude = new List<string> { SpModel.SpLong }
        }));
 
        Assert.Null(exception);
        var modelFromDb = db.SpModels.OrderByDescending(y => y.Id).First();
        Assert.Equal(new DateTime(2021, 02, 14), db.Entry(modelFromDb).Property(SpModel.SpDateTime).CurrentValue);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.Sqlite)]
    public void BulkInsertOrUpdate_EntityWithShadowProperties_PropertiesToExcludeOnUpdate_DoesNotThrow(SqlType dbServer)
    {
        var options = new ContextUtil(dbServer)
            .GetOptions<SpDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_ShadowProperties_ExcludeOnUpdate");

        using var db = new SpDbContext(options);
        var data = GetTestData(db, true, 10).ToList();

        db.BulkInsert(data, new BulkConfig { EnableShadowProperties = true });

        var original = db.SpModels.First();
        var entity = new SpModel { Id = original.Id };
        db.Entry(entity).Property(SpModel.SpLong).CurrentValue = 42;
        db.Entry(entity).Property(SpModel.SpDateTime).CurrentValue = new DateTime(2022, 01, 01);

        var exception = Record.Exception(() => db.BulkInsertOrUpdate(new List<SpModel> { entity }, new BulkConfig
        {
            EnableShadowProperties = true,
            PropertiesToExcludeOnUpdate = new List<string> { SpModel.SpLong }
        }));

        Assert.Null(exception);
        var modelFromDb = db.SpModels.Single(x => x.Id == entity.Id);
        Assert.Equal(new DateTime(2022, 01, 01), db.Entry(modelFromDb).Property(SpModel.SpDateTime).CurrentValue);
    }

    private static IEnumerable<SpModel> GetTestData(DbContext db, bool useEf, int count)
    {
        var data = new List<SpModel>();

        for (int i = 0; i < count; i++)
        {
            var one = new SpModel();

            if (useEf)
            {
                db.Entry(one).Property(SpModel.SpLong).CurrentValue = (long)10;
                db.Entry(one).Property(SpModel.SpNullableLong).CurrentValue = null;
                db.Entry(one).Property(SpModel.SpDateTime).CurrentValue = new DateTime(2021, 02, 14);
            }

            data.Add(one);
        }

        return data; 
    }
}
