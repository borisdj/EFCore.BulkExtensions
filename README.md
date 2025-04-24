# EFCore.BulkExtensions
EntityFrameworkCore extensions (performance improvement - into overdrive):  
-Bulk operations (super fast): **Insert, Update, Delete, Read, Upsert, Sync, SaveChanges.**  
-Batch ops: **Update, Delete** - Deprecated from EF8 since EF7+ has native Execute-Up/Del.  
-AddOp (additional): **Truncate.**  
Library is Lightweight and very Efficient (warp speed), having all mostly used [CRUD](https://en.wikipedia.org/wiki/Create,_read,_update_and_delete) operation.  
Was selected in top 20 [EF Core Extensions](https://docs.microsoft.com/en-us/ef/core/extensions/) recommended by Microsoft.  
Latest version is using EF Core 9.  
Supports all 5 major sql databases: **SQLServer, PostgreSQL, MySQL, Oracle, SQLite**  
Check out [Testimonials](https://docs.google.com/spreadsheets/d/e/2PACX-1vShdv2sTm3oQfowm9kVIx-PLBCk1lGQEa9E6n92-dX3pni7-XQUEp6taVcMSZVi9BaSAizv1YanWTy3/pubhtml?gid=801420190&single=true) from the Community and User Comments.  
With thousands of pleased users and many satisfied clients from around the globe.  
Icon>> and Logo (__):  
<img src="/EFCore.BulkExtensions/EFCoreBulk.png" height=60> & <img src="EFCoreBulkLogo_small.png" height=60>  
(f.forward | rocket time)

**Also take a look into others packages:</br>
Open source (MIT or cFOSS) authored [.Net libraries](https://infopedia.io/dot-net-libraries/) and other projects (@[**Infopedia.io**](https://infopedia.io/) personal blog post)
| №  | Project                  | Type | Description                                              |
| -  | ------------------------ | ---- | -------------------------------------------------------- |
| 1* | [EFCore.BulkExtensions](https://github.com/borisdj/EFCore.BulkExtensions) | .Net_Lib(ngt) cF | EF Core Bulk CRUD Ops (**Flagship** Lib) |
| 2  | [EFCore.UtilExtensions](https://github.com/borisdj/EFCore.UtilExtensions) | .Net_Lib(ngt) MIT | EF Core Custom Annotations and AuditInfo |
| 3  | [EFCore.FluentApiToAnnotation](https://github.com/borisdj/EFCore.FluentApiToAnnotation) | .Net_Lib(ngt) MIT | Converting FluentApi configuration to Annotations |
| 4  | [ExcelIO.FastMapper](https://github.com/borisdj/ExcelIO.FastMapper) | .Net_Lib(ngt) MIT | Excel I/O Mapper to-from Poco & .xlsx + attribute |
| 5  | [FixedWidthParserWriter](https://github.com/borisdj/FixedWidthParserWriter) | .Net_Lib(ngt) MIT | Reading & Writing fixed-width/flat data files |
| 6  | [CsCodeGenerator](https://github.com/borisdj/CsCodeGenerator) | .Net_Lib(ngt) MIT | C# code generation based on Classes and elements |
| 7  | [CsCodeExample](https://github.com/borisdj/CsCodeExample) | C# Code --- MIT | Examples of C# code in form of a simple tutorial |

## License
BulkExtensions [licensed](https://github.com/borisdj/EFCore.BulkExtensions/blob/master/LICENSE.txt) under [**Dual License**](https://codis.tech/efcorebulk) (**cFOSS**: *conditionallyFree* OSS - [**OpenSource Sustainability**](https://infopedia.io/solution-to-opensource-sustainability/) & funding).  
If you do not meet criteria for free usage of software with community license then you have to buy commercial one.  
If eligible for free usage but still need  active support, consider purchasing Starter Lic.  

## Support
If you find this project useful you can mark it by leaving a Github **Star** :star:  
And even with Community license, if you want help Development, you can make a Donation:  
[<img src="https://www.buymeacoffee.com/assets/img/custom_images/yellow_img.png" alt="Buy Me A Coffee" height=28>](https://www.buymeacoffee.com/boris.dj) _ or _ 
[![Button](https://img.shields.io/badge/donate-Bitcoin-orange.svg?logo=bitcoin):zap:](https://borisdj.net/donation/donate-btc.html) ([Moneylution](https://infopedia.io/revolution-of-money/))

## Contributing
Please read [CONTRIBUTING](https://github.com/borisdj/EFCore.BulkExtensions/blob/master/CONTRIBUTING.md) for details on code of conduct, and the process for submitting pull requests. <!-- valid link short also (CONTRIBUTING.md) -->   
When opening issues do write detailed explanation of the problem or feature with reproducible example.  
Want to **Contact** for Development & Consulting: [www.codis.tech](http://www.codis.tech) (*Quality Delivery*)  

## Description
Supported databases:  
-**SQLServer** (or AzureSQL) under the hood uses [SqlBulkCopy](https://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqlbulkcopy.aspx) for Insert, Update/Delete = BulkInsert + raw Sql [MERGE](https://docs.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql).  
-**PostgreSQL** (9.5+) is using [COPY BINARY](https://www.postgresql.org/docs/9.2/sql-copy.html) combined with [ON CONFLICT](https://www.postgresql.org/docs/10/sql-insert.html#SQL-ON-CONFLICT) for Update.  
-**MySQL** (8+) is using [MySqlBulkCopy](https://mysqlconnector.net/api/mysqlconnector/mysqlbulkcopytype/) combined with [ON DUPLICATE](https://dev.mysql.com/doc/refman/8.0/en/insert-on-duplicate.html) for Update.  
-**Oracle** (8+) is using [OracleBulkCopy](https://docs.oracle.com/cd/E11882_01/win.112/e23174/OracleBulkCopyClass.htm#ODPNT7446) combined with [MERGE](https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/MERGE.html) for Update.  
-**SQLite** has no Copy tool, instead library uses [plain SQL](https://learn.microsoft.com/en-us/dotnet/standard/data/sqlite/bulk-insert) combined with [UPSERT](https://www.sqlite.org/lang_UPSERT.html).  
Bulk Tests can not have UseInMemoryDb because InMemoryProvider does not support Relational-specific methods.  
Instead Test options are  SqlServer(Developer or Express), LocalDb(if alongside [Developer v.](https://stackoverflow.com/questions/42885377/sql-server-2016-developer-version-can-not-connect-to-localdb-mssqllocaldb?noredirect=1&lq=1)), or with  other adapters.

## Installation
Available on [![NuGet](https://img.shields.io/nuget/v/EFCore.BulkExtensions.svg)](https://www.nuget.org/packages/EFCore.BulkExtensions/)  [![Downloads](https://img.shields.io/nuget/dt/EFCore.BulkExtensions.svg)](https://www.nuget.org/packages/EFCore.BulkExtensions/)  
Main nuget is for all Databases, and specific ones with single provider for those who need small packages.  
Package manager console command to install: *Install-Package EFCore.BulkExtensions*  
Specific ones have adapter suffix: MainNuget + *.SqlServer/PostgreSql/MySql/Oracle/Sqlite* 
(
[![](https://img.shields.io/static/v1?label=&message=MS&color=darkred)](https://www.nuget.org/packages/EFCore.BulkExtensions.SqlServer)
[![](https://img.shields.io/static/v1?label=&message=PG&color=blue)](https://www.nuget.org/packages/EFCore.BulkExtensions.PostgreSql)
[![](https://img.shields.io/static/v1?label=&message=MY&color=chocolate)](https://www.nuget.org/packages/EFCore.BulkExtensions.MySql)
[![](https://img.shields.io/static/v1?label=&message=OR&color=red)](https://www.nuget.org/packages/EFCore.BulkExtensions.Oracle)
[![](https://img.shields.io/static/v1?label=&message=LT&color=lightgreen)](https://www.nuget.org/packages/EFCore.BulkExtensions.Sqlite)
)  
Its assembly is [Strong-Named](https://docs.microsoft.com/en-us/dotnet/standard/library-guidance/strong-naming) and [Signed](https://github.com/borisdj/EFCore.BulkExtensions/issues/161) with a key.
| Nuget | Target          | Used EF v.| For projects targeting          |
| ----- | --------------- | --------- | ------------------------------- |
| 9.x   | Net 9.0         | EF Core 9 | Net 9.0+                        |
| 8.x   | Net 8.0         | EF Core 8 | Net 8.0+                        |
| 7.x   | Net 6.0         | EF Core 7 | Net 7.0+ or 6.0+                |
| 6.x   | Net 6.0         | EF Core 6 | Net 6.0+                        |
| 5.x   | NetStandard 2.1 | EF Core 5 | Net 5.0+                        |
| 3.x   | NetStandard 2.0 | EF Core 3 | NetCore(3.0+) or NetFrm(4.6.1+) [info](https://github.com/borisdj/EFCore.BulkExtensions/issues/271#issuecomment-567117488)|
| 2.x   | NetStandard 2.0 | EF Core 2 | NetCore(2.0+) or NetFrm(4.6.1+) |
| 1.x   | NetStandard 1.4 | EF Core 1 | NetCore(1.0+)                   |

Supports follows official [.Net lifecycle](https://dotnet.microsoft.com/en-us/platform/support/policy/dotnet-core), currently v.9 as latest and v.8(LTS).  
**Currently *Pomelo.EntityFrameworkCore.MySql* still does not have full Release for EF9 so its nuget is published as 'rc' and Main package as 9.0.0-rc.1 (mysql adapter is ommited from from main release version 9.0.1)

## Usage
It's pretty simple and straightforward.  
**Bulk** Extensions are made on *DbContext* and are used with entities List (supported both regular and Async methods):
```C#
context.BulkInsert(entities);                 context.BulkInsertAsync(entities);
context.BulkInsertOrUpdate(entities);         context.BulkInsertOrUpdateAsync(entities);    //Upsert
context.BulkInsertOrUpdateOrDelete(entities); context.BulkInsertOrUpdateOrDeleteAsync(entiti);//Sync
context.BulkUpdate(entities);                 context.BulkUpdateAsync(entities);
context.BulkDelete(entities);                 context.BulkDeleteAsync(entities);
context.BulkRead(entities);                   context.BulkReadAsync(entities);
context.BulkSaveChanges();                    context.BulkSaveChangesAsync();
```

**-MySQL** when running its Test for the first time execute sql command ([local-data](https://stackoverflow.com/questions/59993844/error-loading-local-data-is-disabled-this-must-be-enabled-on-both-the-client)): `SET GLOBAL local_infile = true;`  
**-SQLite** requires package: [*SQLitePCLRaw.bundle_e_sqlite3*](https://docs.microsoft.com/en-us/dotnet/standard/data/sqlite/custom-versions?tabs=netcore-cli) with call to `SQLitePCL.Batteries.Init()`  

**Batch** Extensions are made on *IQueryable* DbSet and can be used as in the following code segment.  
They are done as pure sql and no check is done whether some are prior loaded in memory and are being Tracked.  
(*updateColumns* is optional param in which PropertyNames added explicitly when need update to it's default value)  
Info about [lock-escalation](https://docs.microsoft.com/en-us/troubleshoot/sql/performance/resolve-blocking-problems-caused-lock-escalation) in SQL Server with Batch iteration example as a solution at the bottom of code segment.
```C#
// Delete
context.Items.Where(a => a.ItemId >  500).BatchDelete();
context.Items.Where(a => a.ItemId >  500).BatchDeleteAsync();

// Update (using Expression arg.) supports Increment/Decrement 
context.Items.Where(a => a.ItemId <= 500).BatchUpdate(a => new Item { Quantity = a.Quantity + 100});
context.Items.Where(a => a.ItemId <= 500).BatchUpdateAsync(a => new Item {Quantity=a.Quantity+100});
  // can be as value '+100' or as variable '+incrementStep' (int incrementStep = 100;)
  
// Update (via simple object)
context.Items.Where(a => a.ItemId <= 500).BatchUpdate(new Item { Description = "Updated" });
context.Items.Where(a => a.ItemId <= 500).BatchUpdateAsync(new Item { Description = "Updated" });
// Update (via simple object) - requires additional Argument for setting to Property default value
var updateCols = new List<string> { nameof(Item.Quantity) }; //Update 'Quantity' to default val:'0'
var q = context.Items.Where(a => a.ItemId <= 500);
int affected = q.BatchUpdate(new Item { Description="Updated" }, updateCols); //result assigned aff.

// Batch iteration (useful in same cases to avoid lock escalation)
do {
    rowsAffected = query.Take(chunkSize).BatchDelete();
} while (rowsAffected >= chunkSize);

// Truncate
context.Truncate<Entity>();
context.TruncateAsync<Entity>();
```

## Performances
Following are performances (in seconds)
* For SQL Server (v. 2019):

| Ops\Rows | EF 100K | Bulk 100K | EF 1 MIL.| Bulk 1 MIL.|
| -------- | ------: | --------: | -------: | ---------: |
| Insert   |  11 s   | 3 s       |   60 s   | 15  s      |
| Update   |   8 s   | 4 s       |   84 s   | 27  s      |
| Delete   |  50 s   | 3 s       | 5340 s   | 15  s      |

TestTable has 6 columns (Guid, string x2, int, decimal?, DateTime), all inserted and 2 were updated.  
Test done locally on configuration: INTEL i7-10510U CPU 2.30GHz, DDR3 16 GB, SSD SAMSUNG 512 GB.  
For small data sets there is an overhead since most Bulk ops need to create Temp table and also Drop it after finish.  
Probably good advice would be to use **Bulk ops for sets greater than 1000** (condition in DbContext or Repository).

## Bulk info
If Windows Authentication is used then in ConnectionString there should be *Trusted_Connection=True;* because Sql credentials are required to stay in connection.  
Another Conn.Str. config that can be useful for operations with extremely large data sets is [*ConnectionTimeout*](https://learn.microsoft.com/en-us/dotnet/api/system.data.sqlclient.sqlconnection.connectiontimeout?view=netframework-4.8.1&redirectedfrom=MSDN#System_Data_SqlClient_SqlConnection_ConnectionTimeout) that can be increased from default 15 s to 60 or more to avoid '*Execution Timeout*' if it were to occur.

When used directly each of these operations are separate transactions and are automatically committed.  
And if we need multiple operations in single procedure then explicit transaction should be used, for example:  
```C#
using (var transaction = context.Database.BeginTransaction())
{
    context.BulkInsert(entities1List);
    context.BulkInsert(entities2List);
    transaction.Commit();
}

// or with newer synax as of C# 8.0 (auto disposable objects)
using var transaction = context.Database.BeginTransaction();
context.BulkInsert(entities1List);
context.BulkInsert(entities2List);
transaction.Commit();
```

**BulkInsertOrUpdate** method can be used when there is need for both operations but in one connection to database.  
It makes Update when PK(PrimaryKey) is matched, otherwise does Insert.  

**BulkInsertOrUpdateOrDelete** effectively [synchronizes](https://www.mssqltips.com/sqlservertip/1704/using-merge-in-sql-server-to-insert-update-and-delete-at-the-same-time/) table rows with input data.  
Those in Db that are not found in the list will be deleted.  
Partial Sync can be done on table subset using expression set on config with method:  
`bulkConfig.SetSynchronizeFilter<Item>(a => a.Quantity > 0);`  
Not supported for SQLite (Lite has only UPSERT statement) nor currently for PostgreSQL. Here way to achieve sync functionality is to Select or BulkRead existing data from DB, split list into sublists and call separately Bulk methods for BulkInsertOrUpdate and Delete.

**BulkRead** (SELECT and JOIN done in Sql)  
Used when need to Select from big List based on Unique Prop./Columns specified in config `UpdateByProperties`  
```C#
// instead of WhereIN which will TimeOut for List with over around 40 K records
var entities = context.Items.Where(a=> itemsNames.Contains(a.Name)).AsNoTracking().ToList();//SQL IN
// or JOIN in Memory that loads entire table
var entities = context.Items.Join(itemsNames, a => a.Name, p => p,(a,p)=>a).AsNoTracking().ToList();

// USE
var items = itemsNames.Select(a => new Item { Name = a }).ToList(); // Items list with only Name set
var bulkConfig = new BulkConfig { UpdateByProperties = new List<string> { nameof(Item.Name) } };
context.BulkRead(items, bulkConfig); //Items list will be loaded from Db with data(other properties)
```
Useful config **ReplaceReadEntities** that works as *Contains/IN* and returns all which match the criteria (not unique).  
[Example](https://github.com/borisdj/EFCore.BulkExtensions/issues/733) of special use case when need to BulkRead child entities after BulkReading parent list. 

**SaveChanges** uses Change Tracker to find all modified(CUD) entities and call proper BulkOperations for each table.  
Because it needs tracking it is slower than pure BulkOps but still much faster than regular SaveChanges.  
With config *OnSaveChangesSetFK* setting FKs can be controlled depending on whether PKs are generated in Db or in memory.  
Support for this method was added in version 6 of the library.  
Before calling this method newly created should be added into Range:
```C#
context.Items.AddRange(newEntities); // if newEntities is parent list it can have child sublists
context.BulkSaveChanges();
```
Practical general usage could be made in a way to override regular SaveChanges and if any list of Modified entities entries is greater then say 1000 to redirect to Bulk version.

Note: Bulk ops have optional argument *Type type* that can be set to type of Entity if list has dynamic runtime objects or is inherited from Entity class.

## BulkConfig arguments

**Bulk** methods can have optional argument **BulkConfig** with properties (bool, int, string, object, List<string>):  
```C#
PROPERTY : DEFAULTvalue
----------------------------------------------------------------------------------------------
 1 PreserveInsertOrder: true,                   22 PropertiesToInclude: null,
 2 SetOutputIdentity: false,                    23 PropertiesToIncludeOnCompare: null,
 3 SetOutputNonIdentityColumns: true,           24 PropertiesToIncludeOnUpdate: null,
 4 LoadOnlyIncludedColumns: false,              25 PropertiesToExclude: null,
 5 BatchSize: 2000,                             26 PropertiesToExcludeOnCompare: null,
 6 NotifyAfter: null,                           27 PropertiesToExcludeOnUpdate: null,
 7 BulkCopyTimeout: null,                       28 UpdateByProperties: null,
 8 TrackingEntities: false,                     29 ReplaceReadEntities: false,
 9 UseTempDB: false,                            30 EnableShadowProperties: false,
10 UniqueTableNameTempDb: true,                 31 CustomSqlPostProcess: null,
11 CustomDestinationTableName: null,            32 IncludeGraph: false,
12 CustomSourceTableName: null,                 33 OmitClauseExistsExcept: false,
13 CustomSourceDestinationMappingColumns: null, 34 DoNotUpdateIfTimeStampChanged: false,
14 OnConflictUpdateWhereSql: null,              35 SRID: 4326,
15 WithHoldlock: true,                          36 DateTime2PrecisionForceRound: false,
16 CalculateStats: false,                       37 TemporalColumns: { "PeriodStart", "PeriodEnd" },
17 SqlBulkCopyOptions: Default,                 38 OnSaveChangesSetFK: true,
18 SqlBulkCopyColumnOrderHints: null,           39 IgnoreGlobalQueryFilters: false,
19 DataReader: null,                            40 EnableStreaming: false,
20 UseOptionLoopJoin:false,                     41 ApplySubqueryLimit: 0
21 ConflictOption: None
----------------------------------------------------------------------------------------------
METHOD: SetSynchronizeFilter<T>
        SetSynchronizeSoftDelete<T>
```
If we want to change defaults, BulkConfig should be added explicitly with one or more bool properties set to true, and/or int props like **BatchSize** to different number.   Config also has DelegateFunc for setting *Underlying-Connection/Transaction*, e.g. in UnderlyingTest.  
When doing update we can chose to exclude one or more properties by adding their names into **PropertiesToExclude**, or if we need to update less then half column then **PropertiesToInclude** can be used. Setting both Lists are not allowed.

When using the **BulkInsert_/OrUpdate** methods, you may also specify the **PropertiesToIncludeOnCompare** and **PropertiesToExcludeOnCompare** properties (only for SqlServer). By adding a column name to the *PropertiesToExcludeOnCompare*, will allow it to be inserted and updated but will not update the row if any of the other columns in that row did not change. For example, if you are importing bulk data and want to remove from comparison an internal *CreateDate* or *UpdateDate*, you add those columns to the *PropertiesToExcludeOnCompare*.  
Another option that may be used in the same scenario are the **PropertiesToIncludeOnUpdate** and **PropertiesToExcludeOnUpdate** properties. These properties will allow you to specify insert-only columns such as *CreateDate* and *CreatedBy*.

If we want Insert only new and skip existing ones in Db (Insert_if_not_Exist) then use *BulkInsertOrUpdate* with config
`PropertiesToIncludeOnUpdate = new List<string> { "" }`

Additionally, there is **UpdateByProperties** for specifying custom properties, by which we want update to be done.  
When setting multiple props in UpdateByProps then match done by columns combined, like unique constrains based on those cols.  
Using UpdateByProperties while also having Identity column requires that Id property be [Excluded](https://github.com/borisdj/EFCore.BulkExtensions/issues/131).  
Also, with PostgreSQL when matching is done it requires UniqueIndex so for custom UpdateByProperties that do not have Un.Ind., it is temporarily created in which case method can not be in transaction (throws: *current transaction is aborted; CREATE INDEX CONCURRENTLY cannot run inside a transaction block*).  
Similar is done with MySQL by temporarily adding UNIQUE CONSTRAINT.  

If **NotifyAfter** is not set it will have same value as _BatchSize_ while **BulkCopyTimeout** when not set, has SqlBulkCopy default, which is 30 seconds and if set to 0 it indicates no limit.    
_SetOutputIdentity_ have a purpose only when PK has Identity (usually *int* type with AutoIncrement), while if PK is Guid(sequential) created in Application there is no need for them.  
Also, Tables with Composite Keys have no Identity column, so no functionality for them in that case either.
```C#
var bulkConfig = new BulkConfig { SetOutputIdentity = true, BatchSize = 4000 };
context.BulkInsert(entities, bulkConfig);
context.BulkInsertOrUpdate(entities, new BulkConfig { SetOutputIdentity = true }); //e.g.
context.BulkInsertOrUpdate(entities, b => b.SetOutputIdentity = true); //BulkConfig with Action arg.
```

**PreserveInsertOrder** is **true** by default and makes sure that entities are inserted to Db as ordered in entitiesList.  
When a table has Identity column (int autoincrement) with 0 values in list, they will temporarily be automatically changed from 0s into range -N:-1.  
Or it can be manually set with proper values for order (Negative values used to skip conflict with existing ones in Db).  
Here single Id value itself doesn't matter, db will change it to next in sequence, what matters is their mutual relationship for sorting.  
Insertion order is implemented with [TOP](https://docs.microsoft.com/en-us/sql/t-sql/queries/top-transact-sql) in conjunction with ORDER BY. [so/merge-into-insertion-order](https://stackoverflow.com/questions/884187/merge-into-insertion-order).  
This config should remain true when *SetOutputIdentity* is set to true on Entity containing NotMapped Property. [issues/76](https://github.com/borisdj/EFCore.BulkExtensions/issues/76)  
When using **SetOutputIdentity** Id values will be updated to new ones from database.  
With BulkInsertOrUpdate on SQLServer for those that will be updated it has to match with Id column, or other unique column(s) if using UpdateByProperties in which case  [orderBy done with those props](https://github.com/borisdj/EFCore.BulkExtensions/issues/806) instead of ID, due to how Sql MERGE works. To preserve insert order by Id in this case alternative would be first to use BulkRead and find which records already exist, then split the list into 2 lists entitiesForUpdate and entitiesForInsert without configuring UpdateByProps).  
Also for SQLite combination of BulkInsertOrUpdate and IdentityId automatic set will not work properly since it does [not have full MERGE](https://github.com/borisdj/EFCore.BulkExtensions/issues/556) capabilities like SqlServer. Instead list can be split into 2 lists, and call separately BulkInsert and BulkUpdate.  
  
**SetOutputIdentity** is useful when BulkInsert is done to multiple related tables that have Identity column.  
After Insert is done to the first table, we need Id-s (if using Option 1) that were generated in Db because they are FK(ForeignKey) in second table.  
It is implemented with [OUTPUT](https://docs.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql) as part of MERGE Query, so in this case, even the Insert is not done directly to TargetTable but to TempTable and then Merged with TargetTable.  
When used Id-s will be updated on entitiesList, and if *PreserveInsertOrder* is set to *false* then entitiesList will be cleared and reloaded.  
If Entity has Json column with null value and we set OutputIdentity then also set OutputNonIdentity to false, because [JsonNull](https://github.com/borisdj/EFCore.BulkExtensions/issues/1572) mapping throws an exception.  
**SetOutputNonIdentityColumns** used only when *SetOutputIdentity* is set to true, and if this remains True (which is default) all columns are reloaded from Db.  
When changed to false, only the Identity column is loaded to reduce load back from DB for efficiency.  
  
Example of *SetOutputIdentity* with parent-child FK related tables:
```C#
int numberOfEntites = 1000;
var entities = new List<Item>();
var subEntities = new List<ItemHistory>();
for (int i = 1; i <= numberOfEntites; i++)
{
    var entity = new Item { Name = $"Name {i}" };
    entity.ItemHistories = new List<ItemHistory>()
    {
        new ItemHistory { Remark = $"Info {i}.1" },
        new ItemHistory { Remark = $"Info {i}.2" }
    };
    entities.Add(entity);
}

// Option 1 (recommended)
using (var transaction = context.Database.BeginTransaction())
{
    context.BulkInsert(entities, new BulkConfig { SetOutputIdentity = true });
    foreach (var entity in entities) {
        foreach (var subEntity in entity.ItemHistories) {
            subEntity.ItemId = entity.ItemId; // sets FK to match linked PK that was generated in DB
        }
        subEntities.AddRange(entity.ItemHistories);
    }
    context.BulkInsert(subEntities);
    transaction.Commit();
}

// Option 2 using Graph (only for SQL Server and only for simple relationship parent-child)
// - all entities in relationship with main ones in list are BulkInsertUpdated
context.BulkInsert(entities, b => b.IncludeGraph = true);
  
// Option 3 with BulkSaveChanges() - uses ChangeTracker so little slower then direct Bulk
context.Items.AddRange(entities);
context.BulkSaveChanges();
```
When **CalculateStats** set to True the result returned in `BulkConfig.StatsInfo` (*StatsNumber-Inserted/Updated/Deleted*).  
If used for pure Insert (with Batching) then SetOutputIdentity should also be configured because Merge is required.  
**TrackingEntities** can be set to True if we want to have tracking of entities from BulkRead or if SetOutputIdentity is set.  
**WithHoldlock** means [Serializable isolation](https://github.com/borisdj/EFCore.BulkExtensions/issues/41) level that locks the table (can have negative effect on [concurrency](https://www.linkedin.com/posts/milan-jovanovic_ef-core-doesnt-support-pessimistic-locking-activity-7184445256870825984-QSLU/)).  
_ Setting it False can optionally be used to solve deadlock issue Insert.  
**UseTempDB** when set then BulkOperation has to be [inside Transaction](https://github.com/borisdj/EFCore.BulkExtensions/issues/49).  
**UniqueTableNameTempDb** when changed to false, temp table name will be only 'Temp' without random numbers.  
**CustomDestinationTableName** can be set with 'TableName' only or with 'Schema.TableName'.  
**CustomSourceTableName** when set enables source data from specified table already in Db, so input list not used and can be empty.  
**CustomSourceDestinationMappingColumns** dict can be set only if CustomSourceTableName is configured and it is used for specifying Source-Destination column names when they are not the same. Example in test `DestinationAndSourceTableNameTest`.  
**EnableShadowProperties** to add (normal) Shadow Property and to persist value. Disables automatic discriminator, use manual method.  
**CustomSqlPostProcess** If used, should be set to valid pure Sql syntax, that would be run after main operation but before deleting temporary tables. One practical use case would be to move data from TempOutput table (set UniqueTableNameTempDb to False know the name) into a some Log table, optionally using FOR JSON PATH (example test: *CustomSqlPostProcessTest*).  
**IncludeGraph** when set, all entities that have relations with main ones from the list are also merged into theirs tables.  
**OmitClauseExistsExcept** removes the clause from Merge statement, required when having noncomparable types like XML, and useful when need to activate triggers even for same data.  
_ Also in some [sql collation](https://github.com/borisdj/EFCore.BulkExtensions/issues/641), small and capital letters are considered same (case-insensitive) so for BulkUpdate set it false.  
**DoNotUpdateIfTimeStampChanged** if set checks TimeStamp for Concurrency, ones with conflict will [not be updated](https://github.com/borisdj/EFCore.BulkExtensions/issues/469#issuecomment-803662721).  
Return info will be in *BulkConfig.**TimeStampInfo*** object within field `NumberOfSkippedForUpdate` and list `EntitiesOutput`.  
**SRID** Spatial Reference Identifier - for SQL Server with NetTopologySuite.  
**DateTime2PrecisionForceRound** If dbtype datetime2 has precision less then default 7, example 'datetime2(3)' SqlBulkCopy does Floor instead of Round so when this Property is set then Rounding will be done in memory to make sure inserted values are same as with regular SaveChanges.  
**TemporalColumns** are shadow columns used for Temporal table. Default elements 'PeriodStart' and 'PeriodEnd' can be changed if those columns have custom names.  
**OnSaveChangesSetFK** is used only for BulkSaveChanges. When multiply entries have FK relationship which is Db generated, this set proper value after reading parent PK from Db. IF PK are generated in memory like are some Guid then this can be set to false for better efficiency.  
**ReplaceReadEntities** when set to True result of BulkRead operation will be provided using replace instead of update. Entities list parameter of BulkRead method will be repopulated with obtained data. Enables functionality of Contains/IN which will return all entities matching the criteria (does not have to be by unique columns).  
**UseOptionLoopJoin** when set it appends 'OPTION (LOOP JOIN)' for SqlServer, to reduce potential deadlocks on tables that have FKs. Use this [sql hint](https://learn.microsoft.com/en-us/sql/t-sql/queries/hints-transact-sql-query?view=sql-server-ver16) as a last resort for experienced devs and db admins.  
**ConflictOption**: -*None*(as errors), -*Replace*(conflicting with new rows), -*Ignore*(keep old rows)  
**ApplySubqueryLimit** Default is zero '0'. When set to larger value it appends: LIMIT 'N', to generated query. Used only with PostgreSql.

**DataReader** can be used when DataReader is also configured and when set it is propagated to SqlBulkCopy util object.  
**EnableStreaming** can be set to True if want to have tracking of entities from BulkRead or when SetOutputIdentity is set, useful for big field like blob, binary column.

**SqlBulkCopyOptions** is Enum (only for SqlServer) with [[Flags]](https://stackoverflow.com/questions/8447/what-does-the-flags-enum-attribute-mean-in-c) attribute which enables specifying one or more options:  
*Default, KeepIdentity, CheckConstraints, TableLock, KeepNulls, FireTriggers, UseInternalTransaction*  
If need to set Identity PK in memory, Not let DB do the autoincrement, then need to use **KeepIdentity**:  
`var bulkConfig = new BulkConfig { SqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity };`  
Useful for example when copying from one Db to another.

**OnConflictUpdateWhereSql<T>** To define conditional updates on merges, receives (existingTable, insertedTable).  
--Example: `bc.OnConflictUpdateWhereSql = (ex, in) => $"{in}.TimeUpdated > {ex}.TimeUpdated";`  
**SetSynchronizeFilter<T>** A method that receives and sets expression filter on entities to delete when using BulkInsertOrUpdateOrDelete. Those that are filtered out will be ignored and not deleted.  
**SetSynchronizeSoftDelete<T>** A method that receives and sets expresion on entities to update property instead of deleting when using BulkInsertOrUpdateOrDelete.  
`bulkConfig.SetSynchronizeSoftDelete<SomeObject>(a => new SomeObject { IsDeleted = true });`  

Last optional argument is **Action progress** (Example in *EfOperationTest.cs* *RunInsert()* with *WriteProgress()*).
```C#
context.BulkInsert(entitiesList, null, (a) => WriteProgress(a));
```

For **parallelism**, important notes are:  
-SqlBulk [in Parallel](https://www.adathedev.co.uk/2011/01/sqlbulkcopy-to-sql-server-in-parallel.html)  
-Concurrent operations not run in the [same Context instance](https://learn.microsoft.com/en-us/ef/core/miscellaneous/async)  
-Import data to a single unindexed table with [table level lock](https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2005/ms186341(v=sql.90))  

Library supports [Global Query Filters](https://docs.microsoft.com/en-us/ef/core/querying/filters) and [Value Conversions](https://docs.microsoft.com/en-us/ef/core/modeling/value-conversions) as well.  
Additionally BatchUpdate and named Property works with [EnumToString Conversion](https://github.com/borisdj/EFCore.BulkExtensions/issues/397)  
It can map [OwnedTypes](https://docs.microsoft.com/en-us/ef/core/modeling/owned-entities), also next are links with info how to achieve 
[NestedOwnedTypes](https://github.com/borisdj/EFCore.BulkExtensions/issues/167#issuecomment-476737959) and 
[OwnedInSeparateTable](https://github.com/borisdj/EFCore.BulkExtensions/issues/114#issuecomment-803462928)  
On PG when Enum is in OwnedType it needs to have [Converter explicitly](https://github.com/borisdj/EFCore.BulkExtensions/issues/1108) configured in *OnModelCreating*  

Table splitting is somewhat specific but could be configured in the way [Set TableSplit](https://github.com/borisdj/EFCore.BulkExtensions/issues/352#issuecomment-803674404)  
With [Computed](https://docs.microsoft.com/en-us/ef/core/modeling/relational/computed-columns) and [Timestamp](https://docs.microsoft.com/en-us/ef/core/modeling/concurrency) Columns, it will work in a way that they are automatically excluded from Insert. And when combined with *SetOutputIdentity* they will be Selected.  
[Spatial](https://docs.microsoft.com/en-us/sql/relational-databases/spatial/spatial-data-types-overview?view=sql-server-ver15) types, like Geometry, are also supported and if an Entity has one, clause *EXIST ... EXCEPT* is skipped because it's not comparable.  
Performance for bulk ops measured with `ActivitySources` named: '*BulkExecute*' (tags: '*operationType*', '*entitiesCount*')  
Bulk Extension methods can be [Overridden](https://github.com/borisdj/EFCore.BulkExtensions/issues/56) if required, for example to set AuditInfo.  
If having problems with Deadlock, there is useful info in [issue/46](https://github.com/borisdj/EFCore.BulkExtensions/issues/46).

**TPH** ([Table-Per-Hierarchy](https://docs.microsoft.com/en-us/aspnet/core/data/ef-mvc/inheritance)) inheritance model can be set in 2 ways.  
First is automatically by Convention, in which case the Discriminator column is not directly in the Entity but is [Shadow](https://learn.microsoft.com/en-us/ef/core/modeling/shadow-properties) Property.  
And second is to explicitly define Discriminator property in Entity and configure it with `.HasDiscriminator()`.  
An important remark regarding the first case is that since we can not directly set a Discriminator to certain value we need first to add list of entities to DbSet where it will be set and after that we can call Bulk operation. Note that SaveChanges are not called, and we could optionally turn off TrackingChanges for performance. Example:
```C#
public class Student : Person { ... }
context.Students.AddRange(entities); //adding to Context so Shadow property 'Discriminator' gets set
context.BulkInsert(entities);
```
**TPT** (Table-Per-Type) way it is [supported](https://github.com/borisdj/EFCore.BulkExtensions/issues/493).

## Structure of SourceCode
- Action Flow
```C#
_CLASSES:                    |DbContextBulk|SqlBulk     |ISqlOperations|SqlOperations  |
DbContextBulkExtensions:     |-Transaction:|-Operation: |-Adapter:     |-ServerAdapter:|
_METHODS: Sync/Async========]|[===========]|[==========]|[============]|[=============]|
C { BulkInsert --------------|             |--Insert ---|--Insert -----|--Insert       |
U / BulkInsertOrUpdate ------|             | \                                         |
U | BulkInsertOrUpdateOrDel.-|             |  \                                        |
U \ BulkUpdate --------------|--Execute ---|--Merge ----|--Merge ------|--Merge        |
D { BulkDelete --------------|             | /                                         |
R { BulkRead ----------------|             |--Read -----|--Read -------|--Read         |
- { Truncate ----------------|             |--Truncate -|--Truncate ---|--Truncate     |
```
- Projects Composition:

| Num | Nuget                                  | Reference   | Transitive dep. | Note         | 
| --- | -------------------------------------- | ----------- | --------------- | ------------ | 
| [0] | EFCore.BulkExtensions.Core             |             |                 | shared       |
| [1] | EFCore.BulkExtensions.***SqlServer***  | [0]         |                 | per provider |
| [2] | EFCore.BulkExtensions.***PostgreSql*** | [0]         |                 | per provider |
| [3] | EFCore.BulkExtensions.***MySql***      | [0]         |                 | per provider |
| [4] | EFCore.BulkExtensions.***Oracle***     | [0]         |                 | per provider |
| [5] | EFCore.BulkExtensions.***Sqlite***     | [0]         |                 | per provider |
| [6] | **EFCore.BulkExtensions** - main one   | [1,2,3,4,5] | [0]             | has all      |

EFCore.BulkExtensions is main Project and Nuget that references all other nugets.  
Other per provider projects have only Core dependency and specific adapter implementation with needed packages.

List of all referenced [Links](https://docs.google.com/spreadsheets/d/e/2PACX-1vRE_rO6799VGMhLAldgDZoHJAdbKI0YZ8gweN7oaBG0KCIMtqZwlaeTyB9IMoThb69Tk0bkjIdNNWl_/pubhtml?gid=2013428247&single=true)
