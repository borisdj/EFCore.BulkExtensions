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
        private static WorkOrder WorkOrder1 = new WorkOrder
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

        private static WorkOrder WorkOrder2 = new WorkOrder
        {
            Description = "Fix toilets",
            Asset = new Asset
            {
                Description = "FLUSHMASTER-1",
                Location = "GYM-BLOCK-3"
            },
            WorkOrderSpares =
            {
                new WorkOrderSpare
                {
                    Description = "Plunger",
                    Quantity = 2,
                    Spare = new Spare
                    {
                        PartNumber = "Poo'o'magic 531",
                        Barcode = "544532bbc"
                    }
                },
                new WorkOrderSpare
                {
                    Description = "Crepepele",
                    Quantity = 1,
                    Spare = new Spare
                    {
                        PartNumber = "MZD f",
                        Barcode = "222655"
                    }
                }
            }
        };

        [Theory]
        [InlineData(DbServer.SqlServer)]
        //[InlineData(DbServer.Sqlite)]
        public async Task BulkInsertOrUpdate_EntityWithNestedObjectGraph_SavesGraphToDatabase(DbServer databaseType)
        {
            ContextUtil.DbServer = databaseType;

            using var db = new GraphDbContext(ContextUtil.GetOptions<GraphDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Graph"));

            var testData = this.GetTestData(db).ToList();
            await db.BulkInsertOrUpdateAsync(testData, new BulkConfig
            {
                IncludeGraph = true
            });

            var workOrderQuery = db.WorkOrderSpares
                .Include(y => y.WorkOrder)
                .Include(y => y.WorkOrder.Asset)
                .Include(y => y.Spare);

            foreach (var wos in workOrderQuery)
            {
                Assert.NotNull(wos.WorkOrder);
                Assert.NotNull(wos.WorkOrder.Asset);
                Assert.NotNull(wos.Spare);
            }
        }

        private IEnumerable<WorkOrder> GetTestData(DbContext db)
        {
            yield return WorkOrder1;
            yield return WorkOrder2;

        }

        public void Dispose()
        {
            using var db = new GraphDbContext(ContextUtil.GetOptions<GraphDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Graph"));
            db.Database.EnsureDeleted();
        }
    }
}
