# GaussDB validation — 2026-09-12

Validated with .NET 10.0.2, the GaussDB EF Core provider 10.0.0, and the supplied
localhost:8888 database account. The server reports openGauss 6.0.3, build 4e5c48e7.
Credentials are read from the ignored local test configuration.

## Functional and packaging checks

- Release GaussDB suite: **83 passed, 0 failed, 0 skipped**.
- GaussDB adapter assembly coverage: **94.42% lines**, **85.54% branches**.
- Broader Release selection covering SQL builders and BatchUtil: **109 passed, 1 failed**.
  The failure is the existing SQL Server `DelegateDecompiler_DecompileAsync_WorksAsync`
  integration test: the configured SQL Server is unavailable. All GaussDB cases passed.
- Release GaussDB and Core NuGet packages were generated successfully.
- An independent console application restored those local packages, without project references
  or StrongNamer, and successfully ran insert, generated identity output, update and read
  against a separately created database. It dropped that database afterward.
- Debug/Release builds used warnings as errors and completed with zero warnings/errors.

Behavior coverage includes synchronous and asynchronous CRUD, progress, generated identities,
combined KeepIdentity flags, mixed existing/new identities in either order, nullable matching keys,
database default values, custom enum conversion, renamed shadow properties, timestamp precision,
numeric arrays, composite keys, exact unique-index detection, external transaction rollback,
temporary table reuse, cancellation cleanup, affected-row statistics, source-table column mappings,
BulkRead replacement, single-enumeration inputs, concurrent provider selection, and tagged/unfiltered
batch commands.

## Performance observations

Release measurements from the run **without coverage instrumentation**, using seven-column
entities and the fixture's `Pooling=false` connection configuration:

| Rows | BulkInsertAsync with identity output | Throughput | BulkUpdateAsync |
| ---: | ---: | ---: | ---: |
| 10,000 | 417.2 ms | 23,971 rows/s | 184.7 ms |
| 50,000 | 988.4 ms | 50,585 rows/s | 656.3 ms |

Each case verified persisted row counts, distinct positive identities and every updated value.
These are local observations, including staging and identity materialization, rather than a
general service-level guarantee or a controlled comparison with the previous implementation.
The coverage run is slower because it instruments executed code.

## Reproduction

```powershell
dotnet test EFCore.BulkExtensions.Tests/EFCore.BulkExtensions.Tests.csproj -c Release --filter FullyQualifiedName~GaussDB --collect:"XPlat Code Coverage" --logger "trx;LogFileName=gaussdb-final.trx" --results-directory Nugets/validation
dotnet build EFCore.BulkExtensions.GaussDB/EFCore.BulkExtensions.GaussDB.csproj -c Release
dotnet pack EFCore.BulkExtensions.GaussDB/EFCore.BulkExtensions.GaussDB.csproj -c Release --no-build --no-restore -o Nugets/validation/packages
```

Local evidence is saved under the ignored `Nugets/validation` directory: TRX results,
Cobertura coverage, packages and the independent consumer smoke application.
The complete solution still includes unrelated provider/environment requirements; passing this
suite does not claim that every database provider's integration suite ran successfully.

The full `EFCore.BulkExtensions.sln` restore fails in the existing MySQL project because
Pomelo 9.0.0 requires EF Core Relational 9.x while Core targets 10.0.3 (NU1107).
The .NET 8 aggregate build also fails in unchanged shared code that now uses EF Core 10 APIs:
`RelationalQueryContext.Parameters` and `IReadOnlyPropertyBase.IsCollection`.
GaussDB is a .NET 10 provider; its build and tests do not depend on those projects.
