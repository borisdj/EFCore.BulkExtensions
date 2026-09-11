using GaussDB;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests.GaussDB;

public partial class GaussDBBulkTests
{
    [Fact]
    public async Task BulkOperations_CustomSourceMappings_UpdateReadDeleteAndInsert()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        context.BulkInsert(CreateItems(2, "mapped"));
        context.Database.ExecuteSqlRaw("""
            CREATE TABLE "MappedSource" AS SELECT "Id" AS "SourceId", 333 AS "SourceQuantity" FROM "GaussDbItems" WHERE "Id" = 1;
            """);
        try
        {
            BulkConfig Config() => new()
            {
                CustomSourceTableName = "MappedSource",
                CustomSourceDestinationMappingColumns = new() { ["SourceId"] = "Id", ["SourceQuantity"] = "Quantity" },
                PropertiesToIncludeOnUpdate = [nameof(GaussDbItem.Quantity)]
            };
            await context.BulkUpdateAsync(new List<GaussDbItem>(), Config());
            Assert.Equal(333, context.Items.Single(x => x.Id == 1).Quantity);
            List<GaussDbItem> read = [];
            var readConfig = Config();
            readConfig.ReplaceReadEntities = true;
            await context.BulkReadAsync(read, readConfig);
            Assert.Single(read);
            Assert.Equal(333, read[0].Quantity);
            await context.BulkDeleteAsync(new List<GaussDbItem>(), Config());
            Assert.Equal(1, context.Items.Count());
            context.Database.ExecuteSqlRaw("""
                DROP TABLE "MappedSource";
                CREATE TABLE "MappedSource" AS SELECT "Id" AS "SourceId", "Quantity" AS "SourceQuantity", "Name", "Description", "PriceCents", "UpdatedAt", "Status" FROM "GaussDbItems";
                TRUNCATE "GaussDbItems";
                """);
            await context.BulkInsertAsync(new List<GaussDbItem>(), Config());
            Assert.Equal(1, context.Items.Count());
            Assert.Equal("mapped-2", context.Items.AsNoTracking().Single().Name);
        }
        finally
        {
            context.Database.ExecuteSqlRaw("""DROP TABLE "MappedSource";""");
        }
    }

    [Fact]
    public async Task BatchCommands_TaggedAndUnfiltered_RunAgainstGaussDB()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        context.BulkInsert(CreateItems(3, "batch"));
#pragma warning disable CS0612, CS0618
        var updated = await context.Items.TagWith("GaussDB batch regression").BatchUpdateAsync(new GaussDbItem { Quantity = 77 });
        var deleted = await context.Items.Where(x => x.Quantity == 77).TagWith("GaussDB delete regression").BatchDeleteAsync();
#pragma warning restore CS0612, CS0618
        Assert.Equal(3, updated);
        Assert.Equal(3, deleted);
        Assert.Equal(0, context.Items.Count());
    }

    [Fact]
    public async Task BulkInsert_ConvertersSerialAndTypeModifiers_RoundTrip()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        var rows = new List<GaussDbConvertedItem>
        {
            new() { State = GaussDbStatus.Archived, CreatedAt = new DateTime(2024, 1, 1, 1, 2, 3), Amounts = [1.25m, 20.50m] },
            new() { State = GaussDbStatus.Inactive, CreatedAt = new DateTime(2024, 1, 2, 1, 2, 3), Amounts = [] }
        };
        var config = new BulkConfig { SetOutputIdentity = true, EnableShadowProperties = true,
            ShadowPropertyValue = (_, _) => GaussDbStatus.Active };
        await context.BulkInsertAsync(rows, config);
        Assert.All(rows, x => Assert.True(x.Id > 0));
        Assert.Equal("state-Archived", _fixture.ExecuteScalar<string>(
            """SELECT "State" FROM "GaussDbConvertedItems" ORDER BY "Id" LIMIT 1"""));
        Assert.Equal("shadow-Active", _fixture.ExecuteScalar<string>(
            """SELECT "ShadowState" FROM "GaussDbConvertedItems" ORDER BY "Id" LIMIT 1"""));
        var read = await context.Set<GaussDbConvertedItem>().AsNoTracking().OrderBy(x => x.Id).ToListAsync();
        Assert.Equal(rows[0].State, read[0].State);
        Assert.Equal(rows[0].Amounts, read[0].Amounts);
        Assert.Equal(rows[0].CreatedAt, read[0].CreatedAt);
    }

    [Theory]
    [InlineData(10000)]
    [InlineData(50000)]
    public async Task BulkOperations_LargeBatch_PersistsAndUpdatesAllRows(int count)
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        var rows = Enumerable.Range(1, count).Select(i => new GaussDbItem
        {
            Name = "perf-" + i, Description = "performance", Quantity = i, PriceCents = 100,
            UpdatedAt = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc)
        }).ToList();
        var timer = Stopwatch.StartNew();
        await context.BulkInsertAsync(rows, new BulkConfig { SetOutputIdentity = true });
        var insertMs = timer.Elapsed.TotalMilliseconds;
        Assert.Equal(count, context.Items.Count());
        Assert.Equal(count, rows.Select(x => x.Id).Distinct().Count());
        Assert.All(rows, x => Assert.True(x.Id > 0));
        rows.ForEach(x => x.Quantity = 5);
        timer.Restart();
        await context.BulkUpdateAsync(rows);
        var updateMs = timer.Elapsed.TotalMilliseconds;
        Assert.Equal(count, context.Items.Count(x => x.Quantity == 5));
        _output.WriteLine($"GaussDB rows={count}; insert+identity={insertMs:F1} ms ({count / insertMs * 1000:F0} rows/s); update={updateMs:F1} ms");
    }

    [Fact]
    public async Task BulkUpsert_PartialCompositeUniqueKey_UsesConfiguredMatch()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        context.Database.ExecuteSqlRaw("""
            DROP INDEX "IX_GaussDbNaturalKeyItems_Code";
            ALTER TABLE "GaussDbNaturalKeyItems" ADD CONSTRAINT "UQ_CodeVersion" UNIQUE ("Code", "Version");
            """);
        context.BulkInsert(new List<GaussDbNaturalKeyItem> { new() { Code = "code", Name = "old", Version = 1 } });
        await context.BulkInsertOrUpdateAsync(new List<GaussDbNaturalKeyItem> { new() { Code = "code", Name = "new", Version = 2 } },
            new BulkConfig
            {
                UpdateByProperties = [nameof(GaussDbNaturalKeyItem.Code)],
                // GaussDB forbids assigning any unique-index column in ON DUPLICATE KEY UPDATE.
                PropertiesToExcludeOnUpdate = [nameof(GaussDbNaturalKeyItem.Version)]
            });
        Assert.Equal(1, context.NaturalKeyItems.Count());
        Assert.Equal("new", context.NaturalKeyItems.Single().Name);
        Assert.Equal(1, context.NaturalKeyItems.Single().Version);
    }

    private static void ConfigureRegressionModel(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<GaussDbConvertedItem>(entity =>
        {
            entity.ToTable("GaussDbConvertedItems");
            Microsoft.EntityFrameworkCore.GaussDBPropertyBuilderExtensions.UseSerialColumn(entity.Property(x => x.Id));
            entity.Property(x => x.State).HasConversion(v => "state-" + v, v => Enum.Parse<GaussDbStatus>(v.Substring(6))).HasColumnType("text");
            entity.Property<GaussDbStatus>("Shadow").HasColumnName("ShadowState")
                .HasConversion(v => "shadow-" + v, v => Enum.Parse<GaussDbStatus>(v.Substring(7))).HasColumnType("text");
            entity.Property(x => x.CreatedAt).HasColumnType("timestamp(3) without time zone");
            entity.Property(x => x.Amounts).HasColumnType("numeric(10,2)[]");
        });
    }

    public class GaussDbConvertedItem
    {
        public int Id { get; set; }
        public GaussDbStatus State { get; set; }
        public DateTime CreatedAt { get; set; }
        public decimal[] Amounts { get; set; } = [];
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BulkUpsert_MixedIdentity_GeneratesNewIdsAndUpdatesDefaults(bool newRowFirst)
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        var existing = CreateItems(1, "existing");
        context.BulkInsert(existing, new BulkConfig { SetOutputIdentity = true });
        var id = existing[0].Id;
        existing[0].Quantity = 0;
        var added = CreateItems(1, "new")[0];
        added.Quantity = 0;
        List<GaussDbItem> rows = newRowFirst ? [added, existing[0]] : [existing[0], added];

        await context.BulkInsertOrUpdateAsync(rows, new BulkConfig { SetOutputIdentity = true });

        Assert.Equal(id, existing[0].Id);
        Assert.True(added.Id > id);
        Assert.Equal(2, context.Items.Count());
        Assert.All(context.Items.AsNoTracking(), x => Assert.Equal(0, x.Quantity));
    }

    [Fact]
    public async Task BulkRead_ReplaceEntities_ReturnsEveryMatchingRowAndClearsMissing()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        context.BulkInsert(CreateItems(3, "read-all"));
        List<GaussDbItem> rows = [new() { Description = "read-all" }];
        var config = new BulkConfig { ReplaceReadEntities = true, UpdateByProperties = [nameof(GaussDbItem.Description)] };
        await context.BulkReadAsync(rows, config);
        Assert.Equal(3, rows.Count);
        Assert.Equal(3, rows.Select(x => x.Id).Distinct().Count());
        rows.Clear();
        rows.Add(new() { Description = "absent" });
        await context.BulkReadAsync(rows, config);
        Assert.Empty(rows);
    }

    [Fact]
    public async Task BulkInsert_CopyStats_ReportsInsertedRows()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        var config = new BulkConfig { CalculateStats = true };
        await context.BulkInsertAsync(CreateItems(3, "stats"), config);
        Assert.Equal(3, config.StatsInfo!.StatsNumberInserted);
        Assert.Equal(0, config.StatsInfo.StatsNumberUpdated);
    }

    [Fact]
    public async Task BulkUpsert_WithoutUniqueIndex_WorksInsideCallerTransaction()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        context.Database.ExecuteSqlRaw("""DROP INDEX "IX_GaussDbNaturalKeyItems_Code";""");
        await using var transaction = await context.Database.BeginTransactionAsync();
        List<GaussDbNaturalKeyItem> rows = [new() { Code = "code", Name = "first", Version = 1 }];
        var config = new BulkConfig { UpdateByProperties = [nameof(GaussDbNaturalKeyItem.Code)], CalculateStats = true };
        await context.BulkInsertOrUpdateAsync(rows, config);
        Assert.Equal(1, config.StatsInfo!.StatsNumberInserted);
        rows[0].Name = "second";
        await context.BulkInsertOrUpdateAsync(rows, config);
        Assert.Equal(1, config.StatsInfo!.StatsNumberUpdated);
        Assert.Equal("second", (await context.NaturalKeyItems.SingleAsync()).Name);
        await transaction.RollbackAsync();
    }

    [Fact]
    public void SqlAdaptersMapping_ParallelContexts_KeepCorrectProviders()
    {
        Parallel.For(0, 200, i =>
        {
            using DbContext context = i % 2 == 0 ? _fixture.CreateContext() :
                new DbContext(new DbContextOptionsBuilder().UseSqlite("Data Source=:memory:").Options);
            var expected = i % 2 == 0 ? SqlAdapters.SqlType.GaussDB : SqlAdapters.SqlType.Sqlite;
            for (int iteration = 0; iteration < 20; iteration++)
                Assert.Equal(expected, SqlAdapters.SqlAdaptersMapping.DbServer(context).Type);
        });
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BulkInsert_WithOutputIdentity_DuplicateKeyDoesNotUpdate(bool isAsync)
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        var seed = CreateItems(1, "original");
        seed[0].Id = 101;
        context.BulkInsert(seed, new BulkConfig { SqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity });
        var duplicate = CreateItems(1, "replacement");
        duplicate[0].Id = 101;
        var config = new BulkConfig { SetOutputIdentity = true, SqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity };

        if (isAsync)
            await Assert.ThrowsAnyAsync<PostgresException>(() => context.BulkInsertAsync(duplicate, config));
        else
            Assert.ThrowsAny<PostgresException>(() => context.BulkInsert(duplicate, config));

        Assert.Equal("original-1", context.Items.AsNoTracking().Single().Name);
        Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BulkUpdate_DefaultValueColumn_CanSetZero(bool isAsync)
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        var rows = CreateItems(1, "default");
        context.BulkInsert(rows, new BulkConfig { SetOutputIdentity = true });
        rows[0].Quantity = 0;

        if (isAsync) await context.BulkUpdateAsync(rows);
        else context.BulkUpdate(rows);

        Assert.Equal(0, context.Items.AsNoTracking().Single().Quantity);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BulkOperations_NullableMatchKey_ReadUpdateDelete(bool isAsync)
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        var seed = CreateItems(3, "nullable");
        seed[0].Description = null;
        context.BulkInsert(seed);
        var rows = new List<GaussDbItem> { new() { Description = null } };
        BulkConfig Config() => new() { UpdateByProperties = [nameof(GaussDbItem.Description)] };

        if (isAsync) await context.BulkReadAsync(rows, Config());
        else context.BulkRead(rows, Config());
        Assert.Equal("nullable-1", rows[0].Name);

        rows[0].Quantity = 777;
        if (isAsync) await context.BulkUpdateAsync(rows, Config());
        else context.BulkUpdate(rows, Config());
        Assert.Equal(777, context.Items.AsNoTracking().Single(x => x.Description == null).Quantity);

        if (isAsync) await context.BulkDeleteAsync(rows, Config());
        else context.BulkDelete(rows, Config());
        Assert.Equal(2, context.Items.Count());
        Assert.False(context.Items.Any(x => x.Description == null));
    }

    [Fact]
    public async Task BulkUpdateAsync_CancelledDuringCopy_CleansStagingTables()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        var rows = CreateItems(10, "cancel");
        context.BulkInsert(rows, new BulkConfig { SetOutputIdentity = true });
        using var cancellation = new CancellationTokenSource();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            context.BulkUpdateAsync(rows, new BulkConfig { NotifyAfter = 1 },
                _ => cancellation.Cancel(), cancellationToken: cancellation.Token));

        Assert.Equal(0, _fixture.ExecuteScalar<long>(
            """SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'GaussDbItemsTemp%'"""));
        Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
        Assert.Equal(10, context.Items.Count());
    }

    [Fact]
    public async Task BulkOperations_UseTempDb_KeepExternalTransactionAndAllowReuse()
    {
        _fixture.ResetSchema();
        await using var context = _fixture.CreateContext();
        await using var transaction = await context.Database.BeginTransactionAsync();
        var rows = CreateItems(3, "transaction");
        await context.BulkInsertAsync(rows, new BulkConfig { SetOutputIdentity = true, CalculateStats = true, UseTempDB = true, UniqueTableNameTempDb = false });
        rows[0].Quantity = 100;
        await context.BulkUpdateAsync(rows, new BulkConfig { UseTempDB = true, UniqueTableNameTempDb = false });
        Assert.Same(transaction, context.Database.CurrentTransaction);
        Assert.Equal(3, await context.Items.CountAsync());
        await transaction.RollbackAsync();
        Assert.Equal(0, _fixture.ExecuteScalar<long>("""SELECT COUNT(*) FROM "GaussDbItems" """));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BulkInsert_CustomSourceTable_UsesDatabaseRows(bool isAsync)
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        context.BulkInsert(CreateItems(3, "source"));
        context.Database.ExecuteSqlRaw("""CREATE TABLE "GaussDbSource" AS TABLE "GaussDbItems"; TRUNCATE "GaussDbItems";""");
        try
        {
            var config = new BulkConfig { CustomSourceTableName = "GaussDbSource" };
            if (isAsync) await context.BulkInsertAsync(new List<GaussDbItem>(), config);
            else context.BulkInsert(new List<GaussDbItem>(), config);
            Assert.Equal(3, context.Items.Count());
        }
        finally
        {
            context.Database.ExecuteSqlRaw("""DROP TABLE "GaussDbSource";""");
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BulkInsert_SingleEnumerationSource_PersistsAllRows(bool isAsync)
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        var rows = new SingleEnumeration<GaussDbItem>(CreateItems(3, "enumerate"));
        if (isAsync) await context.BulkInsertAsync(rows);
        else context.BulkInsert(rows);
        Assert.Equal(3, context.Items.Count());
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BulkUpdateAndDelete_StatsCountOnlyAffectedRows(bool isAsync)
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        var rows = CreateItems(3, "affected");
        context.BulkInsert(rows, new BulkConfig { SetOutputIdentity = true });
        rows[2].Id = 999;
        var update = new BulkConfig { CalculateStats = true };
        if (isAsync) await context.BulkUpdateAsync(rows, update);
        else context.BulkUpdate(rows, update);
        Assert.Equal(0, update.StatsInfo!.StatsNumberInserted);
        Assert.Equal(2, update.StatsInfo.StatsNumberUpdated);
        var delete = new BulkConfig { CalculateStats = true };
        if (isAsync) await context.BulkDeleteAsync(rows, delete);
        else context.BulkDelete(rows, delete);
        Assert.Equal(2, delete.StatsInfo!.StatsNumberDeleted);
        Assert.Equal(0, delete.StatsInfo.StatsNumberInserted);
    }

    private sealed class SingleEnumeration<T>(IEnumerable<T> source) : IEnumerable<T>
    {
        private bool _enumerated;
        public IEnumerator<T> GetEnumerator()
        {
            if (_enumerated) throw new InvalidOperationException("The source was enumerated more than once.");
            _enumerated = true;
            return source.GetEnumerator();
        }
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
