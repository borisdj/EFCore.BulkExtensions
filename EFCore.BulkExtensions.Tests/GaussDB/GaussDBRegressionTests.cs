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
        await context.BulkInsertAsync(rows, new BulkConfig { SetOutputIdentity = true, UseTempDB = true, UniqueTableNameTempDb = false });
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
