using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.Tests.IncludeGraph.Model;
using EFCore.BulkExtensions.Tests.ShadowProperties;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests.IncludeGraph
{
    public class IncludeGraphTests : IDisposable
    {
        [Theory]
        [InlineData(DbServer.SqlServer)]
        //[InlineData(DbServer.Sqlite)]
        public void BulkInsertOrUpdate_EntityWithNestedObjectGraph_SavesGraphToDatabase(DbServer databaseType)
        {
            ContextUtil.DbServer = databaseType;

            using var db = new GraphDbContext(ContextUtil.GetOptions<GraphDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Graph"));

            db.BulkInsertOrUpdate(this.GetTestData(db).ToList(), new BulkConfig
            {
                IncludeGraph = true,
                EnableShadowProperties = true
            });

            Assert.True(db.WorkOrderSpares.Any());
        }

        private IEnumerable<WorkOrder> GetTestData(DbContext db)
        {
            var one = new WorkOrder
            {
                Description = "Fix belt",
                Asset = new Asset
                {
                    Description = "MANU-1",
                    Location = "WAREHOUSE-1"
                },
                WorkOrderSpares =
                {
                    new WorkOrderSpare
                    {
                        Description = "Bolt 5mm x5",
                        Quantity = 5,
                        Spare = new Spare
                        {
                            PartNumber = "MZD 5mm",
                            Barcode = "12345"
                        }
                    },
                    new WorkOrderSpare
                    {
                        Description = "Bolt 10mm x5",
                        Quantity = 5,
                        Spare = new Spare
                        {
                            PartNumber = "MZD 10mm",
                            Barcode = "222655"
                        }
                    }
                }
            };

            yield return one;
        }

        public void Dispose()
        {
            using var db = new GraphDbContext(ContextUtil.GetOptions<GraphDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Graph"));
            db.Database.EnsureDeleted();
        }
    }
}
