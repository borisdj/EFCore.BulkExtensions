using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests.ShadowProperties
{
    public class ShadowPropertyTests : IDisposable
    {
        [Theory]
        [InlineData(DbServer.SQLServer)]
        [InlineData(DbServer.SQLite)]
        public void BulkInsertOrUpdate_EntityWithShadowProperties_SavesToDatabase(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;

            using var db = new SpDbContext(ContextUtil.GetOptions<SpDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_ShadowProperties"));

            db.BulkInsertOrUpdate(this.GetTestData(db, true, 10000).ToList(), new BulkConfig
            {
                EnableShadowProperties = true
            });

            var modelFromDb = db.SpModels.OrderByDescending(y => y.Id).First();
            Assert.Equal((long)10, db.Entry(modelFromDb).Property(SpModel.SpLong).CurrentValue);
            Assert.Null(db.Entry(modelFromDb).Property(SpModel.SpNullableLong).CurrentValue);

            Assert.Equal(new DateTime(2021, 02, 14), db.Entry(modelFromDb).Property(SpModel.SpDateTime).CurrentValue);
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        [InlineData(DbServer.SQLite)]
        public void BulkInsertOrUpdate_EntityWithShadowProperties_GlobalFunc_SavesToDatabase(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;

            using var db = new SpDbContext(ContextUtil.GetOptions<SpDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_ShadowProperties"));

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

        private IEnumerable<SpModel> GetTestData(DbContext db, bool useEf, int count)
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

        public void Dispose()
        {
            using var db = new SpDbContext(ContextUtil.GetOptions<SpDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_ShadowProperties"));
            db.Database.EnsureDeleted();
        }
    }
}
