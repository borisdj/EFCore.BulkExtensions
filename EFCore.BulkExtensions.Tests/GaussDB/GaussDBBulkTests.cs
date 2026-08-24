using EFCore.BulkExtensions.SqlAdapters;
using EFCore.BulkExtensions.SqlAdapters.GaussDB;
using GaussDB;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests.GaussDB;

public class GaussDBBulkTests : IClassFixture<GaussDBBulkTests.GaussDBFixture>
{
    private readonly GaussDBFixture _fixture;

    public GaussDBBulkTests(GaussDBFixture fixture)
    {
        _fixture = fixture;
    }

    [Fact]
    public void SqlAdaptersMapping_ResolvesGaussDBServerFromProvider()
    {
        using var context = _fixture.CreateContext();

        var server = SqlAdaptersMapping.DbServer(context);

        Assert.Equal(SqlType.GaussDB, server.Type);
        Assert.IsType<GaussDBAdapter>(server.Adapter);
        Assert.True(context.Database.IsGaussDB());
    }

    [Fact]
    public void BulkInsert_InsertsRowsAndReportsProgress()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        var items = CreateItems(6, "insert");
        var progress = new List<decimal>();

        context.BulkInsert(items, new BulkConfig { NotifyAfter = 2 }, progress.Add);

        Assert.Equal(6, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbItems"""));
        Assert.Equal(3, progress.Count);
        Assert.Equal(1m, progress.Last());
        Assert.Equal(0, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbItems"" WHERE ""Status"" = 'Archived'"));
    }

    [Fact]
    public async Task BulkInsertAsync_InsertsRows()
    {
        await _fixture.ResetSchemaAsync();
        await using var context = _fixture.CreateContext();
        var items = CreateItems(4, "insert-async");

        await context.BulkInsertAsync(items);

        Assert.Equal(4, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbItems"""));
        Assert.Equal("insert-async-1", _fixture.ExecuteScalar<string>(@"SELECT ""Name"" FROM ""GaussDbItems"" ORDER BY ""Id"" LIMIT 1"));
    }

    [Fact]
    public void BulkInsertOrUpdate_UpdatesExistingAndInsertsNewRows()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        context.BulkInsert(CreateItems(3, "seed"));

        var rows = CreateItems(2, "upsert-updated");
        rows[0].Id = 1;
        rows[0].Quantity = 500;
        rows[1].Id = 2;
        rows[1].Quantity = 600;
        rows.Add(new GaussDbItem
        {
            Name = "upsert-inserted",
            Description = "new row",
            Quantity = 700,
            PriceCents = 725,
            Status = GaussDbStatus.Archived,
            UpdatedAt = new DateTime(2024, 02, 03, 04, 05, 06, DateTimeKind.Utc)
        });

        context.BulkInsertOrUpdate(rows);

        Assert.Equal(4, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbItems"""));
        Assert.Equal(500, _fixture.ExecuteScalar<int>(@"SELECT ""Quantity"" FROM ""GaussDbItems"" WHERE ""Name"" = 'upsert-updated-1'"));
        Assert.Equal("Archived", _fixture.ExecuteScalar<string>(@"SELECT ""Status"" FROM ""GaussDbItems"" WHERE ""Name"" = 'upsert-inserted'"));
    }

    [Fact]
    public async Task BulkInsertOrUpdateAsync_UsesUpdateByProperties()
    {
        await _fixture.ResetSchemaAsync();
        await using var context = _fixture.CreateContext();
        await context.BulkInsertAsync(new List<GaussDbNaturalKeyItem>
        {
            new() { Code = "sku-1", Name = "old", Version = 1 },
            new() { Code = "sku-2", Name = "keep", Version = 1 }
        });

        var rows = new List<GaussDbNaturalKeyItem>
        {
            new() { Code = "sku-1", Name = "updated", Version = 2 },
            new() { Code = "sku-3", Name = "inserted", Version = 1 }
        };

        await context.BulkInsertOrUpdateAsync(rows, new BulkConfig
        {
            UpdateByProperties = new List<string> { nameof(GaussDbNaturalKeyItem.Code) }
        });

        Assert.Equal(3, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbNaturalKeyItems"""));
        Assert.Equal(2, _fixture.ExecuteScalar<int>(@"SELECT ""Version"" FROM ""GaussDbNaturalKeyItems"" WHERE ""Code"" = 'sku-1'"));
        Assert.Equal("inserted", _fixture.ExecuteScalar<string>(@"SELECT ""Name"" FROM ""GaussDbNaturalKeyItems"" WHERE ""Code"" = 'sku-3'"));
    }

    [Fact]
    public void BulkUpdate_UpdatesSelectedRows()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        context.BulkInsert(CreateItems(5, "before-update"));

        var rows = CreateItems(3, "before-update");
        for (int i = 0; i < rows.Count; i++)
        {
            rows[i].Id = i + 1;
        }
        foreach (var item in rows)
        {
            item.Description = "bulk-updated";
            item.Status = GaussDbStatus.Archived;
        }

        context.BulkUpdate(rows);

        Assert.Equal(3, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbItems"" WHERE ""Description"" = 'bulk-updated'"));
        Assert.Equal(3, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbItems"" WHERE ""Status"" = 'Archived'"));
    }

    [Fact]
    public async Task BulkUpdateAsync_UpdatesRows()
    {
        await _fixture.ResetSchemaAsync();
        await using var context = _fixture.CreateContext();
        await context.BulkInsertAsync(CreateItems(3, "before-update-async"));

        var rows = CreateItems(3, "before-update-async");
        for (int i = 0; i < rows.Count; i++)
        {
            rows[i].Id = i + 1;
        }
        foreach (var item in rows)
        {
            item.PriceCents += 1000;
        }

        await context.BulkUpdateAsync(rows);

        Assert.Equal(3, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbItems"" WHERE ""PriceCents"" >= 1100"));
    }

    [Fact]
    public void BulkDelete_DeletesRows()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        context.BulkInsert(CreateItems(5, "before-delete"));

        var rows = new List<GaussDbItem> { new() { Id = 1 }, new() { Id = 2 } };

        context.BulkDelete(rows);

        Assert.Equal(3, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbItems"""));
        Assert.Equal(0, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbItems"" WHERE ""Id"" IN (1, 2)"));
    }

    [Fact]
    public async Task BulkDeleteAsync_DeletesRows()
    {
        await _fixture.ResetSchemaAsync();
        await using var context = _fixture.CreateContext();
        await context.BulkInsertAsync(CreateItems(5, "before-delete-async"));

        var rows = new List<GaussDbItem> { new() { Id = 4 }, new() { Id = 5 } };

        await context.BulkDeleteAsync(rows);

        Assert.Equal(3, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbItems"""));
        Assert.Equal(0, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbItems"" WHERE ""Id"" IN (4, 5)"));
    }

    [Fact]
    public void BulkInsert_WithCompositeKey_InsertsRows()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        var rows = new List<GaussDbCompositeRole>
        {
            new() { UserId = 1, RoleId = 10, Description = "owner" },
            new() { UserId = 1, RoleId = 20, Description = "reader" },
            new() { UserId = 2, RoleId = 10, Description = "writer" }
        };

        context.BulkInsert(rows);

        Assert.Equal(3, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbCompositeRoles"""));
        Assert.Equal("reader", _fixture.ExecuteScalar<string>(@"SELECT ""Description"" FROM ""GaussDbCompositeRoles"" WHERE ""UserId"" = 1 AND ""RoleId"" = 20"));
    }

    [Fact]
    public void Truncate_RemovesRows()
    {
        _fixture.ResetSchema();
        using var context = _fixture.CreateContext();
        context.BulkInsert(CreateItems(3, "truncate"));

        context.Truncate<GaussDbItem>();

        Assert.Equal(0, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbItems"""));
    }

    [Fact]
    public async Task TruncateAsync_RemovesRows()
    {
        await _fixture.ResetSchemaAsync();
        await using var context = _fixture.CreateContext();
        await context.BulkInsertAsync(CreateItems(3, "truncate-async"));

        await context.TruncateAsync<GaussDbItem>();

        Assert.Equal(0, _fixture.ExecuteScalar<long>(@"SELECT COUNT(*) FROM ""GaussDbItems"""));
    }

    [Fact]
    public void ReconfigureTableInfo_UsesSearchPathSchema()
    {
        using var context = _fixture.CreateContext(searchPath: "public");
        var adapter = new GaussDBAdapter();
        var tableInfo = new TableInfo();

        string? schema = adapter.ReconfigureTableInfo(BulkContext.Create(context), tableInfo);

        Assert.Equal("public", schema);
    }

    private static List<GaussDbItem> CreateItems(int count, string prefix)
    {
        var items = new List<GaussDbItem>();
        for (int i = 1; i <= count; i++)
        {
            items.Add(new GaussDbItem
            {
                Name = $"{prefix}-{i}",
                Description = prefix,
                Quantity = i,
                PriceCents = i * 100 + 25,
                Status = i % 2 == 0 ? GaussDbStatus.Inactive : GaussDbStatus.Active,
                UpdatedAt = new DateTime(2024, 01, i, 10, 00, 00, DateTimeKind.Utc)
            });
        }

        return items;
    }

    public sealed class GaussDBFixture : IDisposable
    {
        private const string DatabaseName = "efcore_bulkextensions_gaussdb_tests";
        private readonly string _databaseConnectionString;
        private readonly string _postgresConnectionString;

        public GaussDBFixture()
        {
            _databaseConnectionString = ContextUtil.GetGaussDBConnectionString(DatabaseName);
            _postgresConnectionString = ContextUtil.GetGaussDBConnectionString("postgres");

            RecreateDatabase();
            ResetSchema();
        }

        public GaussDBTestContext CreateContext(string? searchPath = null)
        {
            var connectionString = _databaseConnectionString;
            if (!string.IsNullOrWhiteSpace(searchPath))
            {
                var builder = new GaussDBConnectionStringBuilder(connectionString)
                {
                    SearchPath = searchPath
                };
                connectionString = builder.ConnectionString;
            }

            var options = new DbContextOptionsBuilder<GaussDBTestContext>()
                .UseGaussDB(connectionString)
                .Options;

            return new GaussDBTestContext(options);
        }

        public void ResetSchema()
        {
            using var context = CreateContext();
            context.Database.ExecuteSqlRaw(ResetSchemaSql);
        }

        public async Task ResetSchemaAsync()
        {
            await using var context = CreateContext();
            await context.Database.ExecuteSqlRawAsync(ResetSchemaSql);
        }

        public T ExecuteScalar<T>(string sql)
        {
            using var connection = new GaussDBConnection(_databaseConnectionString);
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            var value = command.ExecuteScalar();
            if (value is null || value is DBNull)
            {
                throw new InvalidOperationException($"SQL scalar query returned no value: {sql}");
            }

            return (T)Convert.ChangeType(value, typeof(T));
        }

        public void Dispose()
        {
            try
            {
                DropDatabase();
            }
            catch (DbException)
            {
                GaussDBConnection.ClearAllPools();
            }
        }

        private void RecreateDatabase()
        {
            DropDatabase();
            ExecuteAgainstPostgres($@"CREATE DATABASE ""{DatabaseName}""");
        }

        private void DropDatabase()
        {
            GaussDBConnection.ClearAllPools();
            ExecuteAgainstPostgres($@"DROP DATABASE IF EXISTS ""{DatabaseName}""");
        }

        private void ExecuteAgainstPostgres(string sql)
        {
            using var connection = new GaussDBConnection(_postgresConnectionString);
            connection.Open();
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }

        private const string ResetSchemaSql = """
DROP TABLE IF EXISTS "GaussDbCompositeRoles";
DROP TABLE IF EXISTS "GaussDbNaturalKeyItems";
DROP TABLE IF EXISTS "GaussDbItems";

CREATE TABLE "GaussDbItems" (
    "Id" serial PRIMARY KEY,
    "Name" character varying(100) NOT NULL,
    "Description" text NULL,
    "Quantity" integer NOT NULL,
    "PriceCents" integer NOT NULL,
    "UpdatedAt" timestamp with time zone NOT NULL,
    "Status" character varying(20) NOT NULL
);

CREATE TABLE "GaussDbNaturalKeyItems" (
    "Id" serial PRIMARY KEY,
    "Code" character varying(64) NOT NULL,
    "Name" character varying(100) NOT NULL,
    "Version" integer NOT NULL
);

CREATE UNIQUE INDEX "IX_GaussDbNaturalKeyItems_Code" ON "GaussDbNaturalKeyItems" ("Code");

CREATE TABLE "GaussDbCompositeRoles" (
    "UserId" integer NOT NULL,
    "RoleId" integer NOT NULL,
    "Description" character varying(100) NOT NULL,
    CONSTRAINT "PK_GaussDbCompositeRoles" PRIMARY KEY ("UserId", "RoleId")
);
""";
    }

    public class GaussDBTestContext : DbContext
    {
        public GaussDBTestContext(DbContextOptions<GaussDBTestContext> options)
            : base(options)
        {
        }

        public DbSet<GaussDbItem> Items => Set<GaussDbItem>();
        public DbSet<GaussDbNaturalKeyItem> NaturalKeyItems => Set<GaussDbNaturalKeyItem>();
        public DbSet<GaussDbCompositeRole> CompositeRoles => Set<GaussDbCompositeRole>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<GaussDbItem>(entity =>
            {
                entity.ToTable("GaussDbItems");
                entity.HasKey(x => x.Id);
                Microsoft.EntityFrameworkCore.GaussDBPropertyBuilderExtensions.UseIdentityColumn(entity.Property(x => x.Id));
                entity.Property(x => x.Name).HasMaxLength(100).IsRequired();
                entity.Property(x => x.Description).HasColumnType("text");
                entity.Property(x => x.Status).HasConversion<string>().HasMaxLength(20);
                entity.Property(x => x.UpdatedAt).HasColumnType("timestamp with time zone");
            });

            modelBuilder.Entity<GaussDbNaturalKeyItem>(entity =>
            {
                entity.ToTable("GaussDbNaturalKeyItems");
                entity.HasKey(x => x.Id);
                Microsoft.EntityFrameworkCore.GaussDBPropertyBuilderExtensions.UseIdentityColumn(entity.Property(x => x.Id));
                entity.HasIndex(x => x.Code).IsUnique();
                entity.Property(x => x.Code).HasMaxLength(64).IsRequired();
                entity.Property(x => x.Name).HasMaxLength(100).IsRequired();
            });

            modelBuilder.Entity<GaussDbCompositeRole>(entity =>
            {
                entity.ToTable("GaussDbCompositeRoles");
                entity.HasKey(x => new { x.UserId, x.RoleId });
                entity.Property(x => x.Description).HasMaxLength(100).IsRequired();
            });
        }
    }

    public class GaussDbItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string? Description { get; set; }
        public int Quantity { get; set; }
        public int PriceCents { get; set; }
        public DateTime UpdatedAt { get; set; }
        public GaussDbStatus Status { get; set; }
    }

    public class GaussDbNaturalKeyItem
    {
        public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public int Version { get; set; }
    }

    public class GaussDbCompositeRole
    {
        public int UserId { get; set; }
        public int RoleId { get; set; }
        public string Description { get; set; } = string.Empty;
    }

    public enum GaussDbStatus
    {
        Active,
        Inactive,
        Archived
    }
}
