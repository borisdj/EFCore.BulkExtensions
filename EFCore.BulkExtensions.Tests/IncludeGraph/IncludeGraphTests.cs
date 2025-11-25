using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.Tests.IncludeGraph.Model;
using EFCore.BulkExtensions.Tests.ShadowProperties;

using Microsoft.EntityFrameworkCore;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

using Xunit;

namespace EFCore.BulkExtensions.Tests.IncludeGraph;

public class IncludeGraphTests : IDisposable
{
    private readonly static WorkOrder WorkOrder1 = new()
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

    private static readonly WorkOrder WorkOrder2 = new()
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
    [InlineData(SqlType.SqlServer)]
    //[InlineData(DbServer.Sqlite)]
    public async Task BulkInsertOrUpdate_EntityWithNestedObjectGraph_SavesGraphToDatabase(SqlType dbServer)
    {
        ContextUtil.DatabaseType = dbServer;

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

        if (WorkOrder1.Asset != null && WorkOrder2.Asset != null)
        {
            WorkOrder1.Asset.WorkOrders.Add(WorkOrder1);
            WorkOrder2.Asset.WorkOrders.Add(WorkOrder2);

            WorkOrder1.Asset.ParentAsset = WorkOrder2.Asset;
            WorkOrder2.Asset.ChildAssets.Add(WorkOrder1.Asset);
        }

        var testData = GetTestData().ToList();
        var bulkConfig = new BulkConfig
        {
            IncludeGraph = true,
            CalculateStats = true,
        };
        await db.BulkInsertOrUpdateAsync(testData, bulkConfig);

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

        Assert.NotNull(bulkConfig.StatsInfo);
        Assert.Equal(12, bulkConfig.StatsInfo.StatsNumberInserted);
    }

    private static IEnumerable<WorkOrder> GetTestData()
    {
        yield return WorkOrder1;
        yield return WorkOrder2;
    }

    [Theory]
    [MemberData(nameof(GetTestData2))]
    public async Task BulkInsertOrUpdate_EntityWithNestedNullableObjectGraph_SavesGraphToDatabase1(IEnumerable<WorkOrder> orders)
    {
        ContextUtil.DatabaseType = SqlType.SqlServer;

        using (var db = new GraphDbContext(ContextUtil.GetOptions<GraphDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Graph")))
        {
            db.Database.EnsureDeleted();
            await db.Database.EnsureCreatedAsync();

            var bulkConfig = new BulkConfig
            {
                IncludeGraph = true,
                CalculateStats = true,
            };
            await db.BulkInsertOrUpdateAsync(orders, bulkConfig);

            Assert.NotNull(bulkConfig.StatsInfo);
            Assert.Equal(
                orders.Count() + orders.Where(x => x.Asset != null).Count(),
                bulkConfig.StatsInfo.StatsNumberInserted);
        }

        using (var db = new GraphDbContext(ContextUtil.GetOptions<GraphDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Graph")))
        {
            var ordersFromDb = db.WorkOrders
                .Include(y => y.Asset)
                .OrderBy(x => x.Id)
                .ToList();

            foreach (var orderFromDb in ordersFromDb)
            {
                var order = orders.First(x => x.Description == orderFromDb.Description);

                if (order.Asset == null)
                    Assert.Null(orderFromDb.Asset);
                else
                    Assert.NotNull(orderFromDb.Asset);
            }
        }
    }

    public static IEnumerable<object[]> GetTestData2()
        =>
        [
            [GetTestDataWithNullInEnd()],
            [GetTestDataWithNullInMiddle()],
            [GetTestDataWithNullInFirst()],
        ];

    private static IEnumerable<WorkOrder> GetTestDataWithNullInEnd()
    {
        var baseData = GetBaseTestData();

        baseData.Last().Asset = null;

        return baseData;
    }

    private static IEnumerable<WorkOrder> GetTestDataWithNullInMiddle()
    {
        var baseData = GetBaseTestData();

        baseData[1].Asset = null;

        return baseData;
    }

    private static IEnumerable<WorkOrder> GetTestDataWithNullInFirst()
    {
        var baseData = GetBaseTestData();

        baseData.First().Asset = null;

        return baseData;
    }

    private static List<WorkOrder> GetBaseTestData()
    {
        return
        [
            new WorkOrder()
            {
                Description = "Fix belt",
                Asset = new Asset
                {
                    Description = "MANU-1",
                    Location = "WAREHOUSE-1"
                },
            },
            new WorkOrder()
            {
                Description = "Fix toilets",
                Asset = new Asset
                {
                    Description = "FLUSHMASTER-1",
                    Location = "GYM-BLOCK-3"
                },
            },
            new WorkOrder()
            {
                Description = "Fix door",
                Asset = new Asset
                {
                    Location = "OFFICE-12"
                },
            }
        ];
    }

    public void Dispose()
    {
        using var db = new GraphDbContext(ContextUtil.GetOptions<GraphDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Graph"));
        db.Database.EnsureDeleted();
    }
}
