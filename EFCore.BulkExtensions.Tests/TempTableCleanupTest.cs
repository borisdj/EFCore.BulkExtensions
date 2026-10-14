using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests;

public class TempTableCleanupTest
{
    private const string TableName = "CLEANUP_ITEM";
    private const string TempTableName = TableName + "Temp";

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.PostgreSql)]
    [InlineData(SqlType.Oracle)]
    public async Task BulkInsertOrUpdateAsync_DropsTempTable_WhenCreateSucceedsButClientGetsError(SqlType sqlType)
    {
        var interceptor = new SqlInterceptor(executed: command =>
        {
            if (IsCreate(command.CommandText) && !command.CommandText.Contains("Output", StringComparison.Ordinal))
                throw new InvalidOperationException("Simulated error after CREATE");
        });
        using var context = await CreateContextAsync(sqlType, interceptor);

        var exception = await RunBulkInsertOrUpdateAsync(context);

        Assert.Equal("Simulated error after CREATE", exception.Message);
        await AssertWorkTablesDroppedAsync(sqlType);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.PostgreSql)]
    [InlineData(SqlType.Oracle)]
    public async Task BulkInsertOrUpdateAsync_DropsWorkTables_WhenCallerCancels(SqlType sqlType)
    {
        using var cts = new CancellationTokenSource();
        using var context = await CreateContextAsync(sqlType, CancelOnMerge(cts));

        var exception = await RunBulkInsertOrUpdateAsync(context, cancellationToken: cts.Token);

        Assert.IsAssignableFrom<OperationCanceledException>(exception);
        await AssertWorkTablesDroppedAsync(sqlType);
    }

    [Theory]
    [InlineData(SqlType.SqlServer)]
    [InlineData(SqlType.PostgreSql)]
    [InlineData(SqlType.Oracle)]
    public async Task BulkInsertOrUpdateAsync_DropsTempTableAndThrowsMergeError_WhenOutputTableDropFails(SqlType sqlType)
    {
        var interceptor = new SqlInterceptor(
            executing: command =>
            {
                if (IsMerge(command.CommandText))
                    throw new InvalidOperationException("Simulated merge error");
            },
            executed: command =>
            {
                if (command.CommandText.Contains("DROP", StringComparison.Ordinal) && command.CommandText.Contains("Output", StringComparison.Ordinal))
                    throw new InvalidOperationException("Simulated DROP error");
            });
        using var context = await CreateContextAsync(sqlType, interceptor);

        var exception = await RunBulkInsertOrUpdateAsync(context);

        Assert.Equal("Simulated merge error", exception.Message);
        await AssertWorkTablesDroppedAsync(sqlType);
    }

    [Fact]
    public async Task BulkInsertOrUpdateAsync_ResetsIdentityInsertAndClosesConnection_WhenCallerCancels()
    {
        using var cts = new CancellationTokenSource();
        using var context = await CreateContextAsync(SqlType.SqlServer, CancelOnMerge(cts));

        var exception = await RunBulkInsertOrUpdateAsync(context, c => c.SqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity, cts.Token);

        Assert.IsAssignableFrom<OperationCanceledException>(exception);
        Assert.Equal(ConnectionState.Closed, context.Database.GetDbConnection().State);
        await AssertWorkTablesDroppedAsync(SqlType.SqlServer);
    }

    [Fact]
    public async Task BulkInsertOrUpdateAsync_ThrowsOriginalError_WhenPostgreSqlTransactionIsAborted()
    {
        var interceptor = new SqlInterceptor(executing: command =>
        {
            if (IsMerge(command.CommandText))
                command.CommandText = "SELECT 1/0";
        });
        using var context = await CreateContextAsync(SqlType.PostgreSql, interceptor);

        await using (await context.Database.BeginTransactionAsync())
        {
            var exception = await RunBulkInsertOrUpdateAsync(context);

            Assert.Equal(PostgresErrorCodes.DivisionByZero, Assert.IsType<PostgresException>(exception).SqlState);
        }

        await AssertWorkTablesDroppedAsync(SqlType.PostgreSql);
    }

    private static bool IsCreate(string sql) => sql.StartsWith("SELECT TOP 0", StringComparison.Ordinal) || sql.StartsWith("CREATE", StringComparison.Ordinal);

    private static bool IsMerge(string sql) => sql.Contains("MERGE ", StringComparison.Ordinal) || sql.Contains("ON CONFLICT", StringComparison.Ordinal);

    private static SqlInterceptor CancelOnMerge(CancellationTokenSource cts) => new(executing: command =>
    {
        if (IsMerge(command.CommandText))
        {
            cts.Cancel();
            throw new OperationCanceledException(cts.Token);
        }
    });

    private static async Task<CleanupContext> CreateContextAsync(SqlType sqlType, SqlInterceptor interceptor)
    {
        using (var context = new CleanupContext(sqlType))
        {
            var creator = context.GetService<IRelationalDatabaseCreator>();
            if (!await creator.ExistsAsync())
                await creator.CreateAsync();
            if (!await TableExistsAsync(context, sqlType, TableName))
                await creator.CreateTablesAsync();
        }
        return new CleanupContext(sqlType, interceptor);
    }

    private static Task<Exception> RunBulkInsertOrUpdateAsync(CleanupContext context, Action<BulkConfig>? configure = null, CancellationToken cancellationToken = default)
    {
        var bulkConfig = new BulkConfig { CalculateStats = true, UniqueTableNameTempDb = false }; // CalculateStats creates the output table on every provider
        configure?.Invoke(bulkConfig);
        var items = new List<CleanupItem> { new() { Id = 1, Name = "TempTableCleanup" } };
        return Assert.ThrowsAnyAsync<Exception>(() => context.BulkInsertOrUpdateAsync(items, bulkConfig, cancellationToken: cancellationToken));
    }

    private static async Task AssertWorkTablesDroppedAsync(SqlType sqlType)
    {
        using var context = new CleanupContext(sqlType);
        foreach (var table in new[] { TempTableName, TempTableName + "Output" })
        {
            Assert.False(await TableExistsAsync(context, sqlType, table), $"{table} was not dropped");
        }
    }

    private static async Task<bool> TableExistsAsync(DbContext context, SqlType sqlType, string table)
    {
        var sql = sqlType switch
        {
            SqlType.SqlServer => @"SELECT CASE WHEN OBJECT_ID({0}) IS NULL THEN 0 ELSE 1 END AS ""Value""",
            SqlType.PostgreSql => @"SELECT CASE WHEN to_regclass('""' || {0} || '""') IS NULL THEN 0 ELSE 1 END AS ""Value""",
            SqlType.Oracle => @"SELECT COUNT(*) AS ""Value"" FROM user_tables WHERE table_name = UPPER({0})",
            _ => throw new NotSupportedException(sqlType.ToString())
        };
        return await context.Database.SqlQueryRaw<int>(sql, table).SingleAsync() == 1;
    }

    public class CleanupItem
    {
        public int Id { get; set; }
        public string? Name { get; set; }
    }

    private sealed class CleanupContext(SqlType sqlType, params IInterceptor[] interceptors)
        : DbContext(new ContextUtil(sqlType).GetOptions<CleanupContext>(interceptors, nameof(TempTableCleanupTest)))
    {
        public DbSet<CleanupItem> Items { get; set; } = null!;

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<CleanupItem>(e =>
            {
                e.ToTable(TableName);
                e.Property(x => x.Id).HasColumnName("ID");
                e.Property(x => x.Name).HasColumnName("NAME").HasMaxLength(50);
            });
        }
    }

    private sealed class SqlInterceptor(Action<DbCommand>? executing = null, Action<DbCommand>? executed = null) : DbCommandInterceptor
    {
        public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
        {
            executing?.Invoke(command);
            return base.NonQueryExecutingAsync(command, eventData, result, cancellationToken);
        }

        public override ValueTask<int> NonQueryExecutedAsync(DbCommand command, CommandExecutedEventData eventData, int result, CancellationToken cancellationToken = default)
        {
            executed?.Invoke(command);
            return base.NonQueryExecutedAsync(command, eventData, result, cancellationToken);
        }
    }
}
