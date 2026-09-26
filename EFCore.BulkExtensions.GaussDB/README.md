# EFCore.BulkExtensions.GaussDB

Bulk insert, update, upsert, delete, read and truncate for the GaussDB EF Core provider.
This project targets .NET 10 and uses `DotNetCore.EntityFrameworkCore.GaussDB 10.0.0`.
It is included in the .NET 10 aggregate package; the .NET 8 projects do not reference it.

```csharp
var options = new DbContextOptionsBuilder<AppDbContext>()
    .UseGaussDB(connectionString)
    .Options;
await using var context = new AppDbContext(options);

await context.BulkInsertAsync(items, new BulkConfig { SetOutputIdentity = true });
await context.BulkUpdateAsync(items);
await context.BulkInsertOrUpdateAsync(items, new BulkConfig { CalculateStats = true });
await context.BulkReadAsync(items);
await context.BulkDeleteAsync(items);
```

Inserts use binary COPY. Staging, DML and output loading share one connection per operation.
Column types, property access paths and value converters are prepared once per batch.
Async operations use asynchronous database I/O and propagate cancellation; owned staging
tables are cleaned up after completion or cancellation. A caller's transaction is never committed
or disposed by the adapter.

`KeepIdentity` can be combined with other `SqlBulkCopyOptions` flags. Without it, mixed upsert
lists containing assigned identities and zero identities generate the new identities per row,
independently of input order. Plain inserts with identity output keep INSERT conflict semantics.
`CalculateStats` reports affected rows, including unmatched update/delete inputs.
`ReplaceReadEntities` requires a `List<T>` and returns every row matching the requested keys.
Nullable matching keys and custom source column mappings are supported for read/update/delete.

For `UseTempDB`, begin a transaction before the bulk operation. Temporary tables are removed
after each operation, allowing the same connection and transaction to be reused.
An upsert using `UpdateByProperties` requires an exact unique key. An existing standalone unique
index is recognized. If none exists, the adapter temporarily creates one; index creation requires
permission and may fail if the current data is not unique. Within a transaction, index creation
uses the non-concurrent form.

GaussDB's `ON DUPLICATE KEY UPDATE` cannot assign primary-key or unique-index columns,
including unique columns outside `UpdateByProperties`. Exclude those additional columns using
`PropertiesToExcludeOnUpdate`, or modify them with a separate `BulkUpdate`.
Upsert matches conflicts on database unique keys; applications with multiple unique constraints
must account for that database behavior. `OnConflictUpdateWhereSql`,
`BulkInsertOrUpdateOrDelete`, and an upsert containing only key columns are unsupported.
The adapter throws instead of silently ignoring these unsupported operations.

The repository disables StrongNamer for projects consuming the GaussDB provider: rewriting
its unsigned dependency binaries changes their identities and breaks EF query compilation.
BulkExtensions' own assemblies remain signed. Consumers must preserve the provider binaries
distributed by NuGet.

Run the integration suite after setting `ConnectionStrings:GaussDB` in the ignored
`EFCore.BulkExtensions.Tests/testsettings.local.json`. The connection string must include
`Database={databaseName}`; the test account needs permission to create and drop the dedicated
`efcore_bulkextensions_gaussdb_tests` database. The fixture recreates that database.
Run only one test process against it at a time.

```powershell
dotnet test EFCore.BulkExtensions.Tests/EFCore.BulkExtensions.Tests.csproj -c Release --filter FullyQualifiedName~GaussDB
```

The latest validation used the GaussDB provider against the supplied localhost:8888 server,
which reports openGauss 6.0.3. See [validation results](VALIDATION.md) for coverage,
performance measurements, packaging checks and environment limits.
