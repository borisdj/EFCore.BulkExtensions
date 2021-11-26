using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.Tests.IncludeGraph.Model;
using EFCore.BulkExtensions.Tests.ShadowProperties;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
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
        [InlineData(DbServer.SQLServer)]
        //[InlineData(DbServer.Sqlite)]
        public async Task BulkInsertOrUpdate_EntityWithNestedObjectGraph_SavesGraphToDatabase(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;

            using var db = new GraphDbContext(ContextUtil.GetOptions<GraphDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Graph"));
            await db.Database.EnsureCreatedAsync();

            // To ensure there are no stack overflows with circular reference trees, we must test for that.
            // Set all navigation properties so the base navigation and its inverse both have values
            foreach (var wos in WorkOrder1.WorkOrderSpares)
            {
                wos.WorkOrder = WorkOrder1;
            }

            foreach (var wos in WorkOrder2.WorkOrderSpares)
            {
                wos.WorkOrder = WorkOrder2;
            }

            WorkOrder1.Asset.WorkOrders.Add(WorkOrder1);
            WorkOrder2.Asset.WorkOrders.Add(WorkOrder2);

            WorkOrder1.Asset.ParentAsset = WorkOrder2.Asset;
            WorkOrder2.Asset.ChildAssets.Add(WorkOrder1.Asset);

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
