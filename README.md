# EFCore.BulkExtensions
EntityFrameworkCore extensions: <br>
-Bulk operations **(Insert, Update, Delete, Read, Upsert, Sync, SaveChanges)**<br>
-Batch ops (**Delete, Update**) and **Truncate**.<br>
Library is Lightweight and very Efficient, having all mostly used CRUD operation.<br>
Was selected in top 20 [EF Core Extensions](https://docs.microsoft.com/en-us/ef/core/extensions/) recommended by Microsoft.<br>
Latest version is using EF Core 6 and targeting .Net 6.<br>
At the moment supports Microsoft SQL Server(2012+) or Sql Azure, PostgreSQL(9.5+) and SQLite.<br>
-SQL Server under the hood uses [SqlBulkCopy](https://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqlbulkcopy.aspx) for Insert, for Update/Delete combines BulkInsert with raw Sql [MERGE](https://docs.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql).<br>
-PostgreSQL is using [COPY BINARY](https://www.postgresql.org/docs/9.2/sql-copy.html) combined with [ON CONFLICT](https://www.postgresql.org/docs/10/sql-insert.html#SQL-ON-CONFLICT) for Update.<br>
-For SQLite there is no Copy tool, instead library uses plain SQL combined with [UPSERT](https://www.sqlite.org/lang_UPSERT.html).<br>
Bulk Tests can not have UseInMemoryDb because InMemoryProvider does not support Relational-specific methods.<br>
Instead Test options are  SqlServer(Developer or Express), LocalDb([if alongside Developer v.](https://stackoverflow.com/questions/42885377/sql-server-2016-developer-version-can-not-connect-to-localdb-mssqllocaldb?noredirect=1&lq=1)), or for other adapters PostgreSQL/SQLite.

<!--[![Button](https://img.shields.io/nuget/v/EFCore.BulkExtensions.svg)](https://www.nuget.org/packages/EFCore.BulkExtensions/)-->
Available on <a href="https://www.nuget.org/packages/EFCore.BulkExtensions/"><img src="https://buildstats.info/nuget/EFCore.BulkExtensions" /></a><br>
Package manager console command for installation: *Install-Package EFCore.BulkExtensions*<br>
| Nuget | Target          | Used EF v.  | For projects targeting          |
| ----- | --------------- | ----------- | ------------------------------- |
| 6.x   | Net 6.0         | EF Core 6.0 | Net 6.0+                        |
| 5.x   | NetStandard 2.1 | EF Core 5.0 | Net 5.0+                        |
| 3.x   | NetStandard 2.0 | EF Core 3.n | NetCore(3.0+) or NetFrm(4.6.1+) [MoreInfo](https://github.com/borisdj/EFCore.BulkExtensions/issues/271#issuecomment-567117488)|
| 2.x   | NetStandard 2.0 | EF Core 2.n | NetCore(2.0+) or NetFrm(4.6.1+) |
| 1.x   | NetStandard 1.4 | EF Core 1.0 | NetCore(1.0+)                   |

## Contributing

If you find this project useful you can mark it by leaving a Github **Star** ⭐.</br>
If you would like to support the Project by making a Donation ($10) *#BuyMeBeer*, you are welcome to do so:<br>
[![Button](https://img.shields.io/badge/donate-PayPal-yellow.svg)](https://www.paypal.me/BorisDjurdjevic/10) or
[![Button](https://img.shields.io/badge/donate-Nano-9cf.svg)](https://borisdj.github.io/pages/donation/donate-nano.html)([0 fee](https://nano.org/))<br>
Please read [CONTRIBUTING](CONTRIBUTING.md) for details on code of conduct, and the process for submitting pull requests.<br>
[![NuGet](https://img.shields.io/npm/l/express.svg)](https://github.com/borisdj/EFCore.BulkExtensions/blob/master/LICENSE)<br>
[**ContactForm**](https://docs.google.com/forms/d/e/1FAIpQLSfcUe15zxZS_YI6zZIt-l3L0mpmQRPUsaoxylfgFTfSVu_gmg/viewform) for Development & Consulting.

## Usage
It's pretty simple and straightforward.<br>
**Bulk** Extensions are made on *DbContext* class and can be used like this (supported both regular and Async methods):
```C#
context.BulkInsert(entitiesList);                 context.BulkInsertAsync(entitiesList);
context.BulkInsertOrUpdate(entitiesList);         context.BulkInsertOrUpdateAsync(entitiesList);      //Upsert
context.BulkInsertOrUpdateOrDelete(entitiesList); context.BulkInsertOrUpdateOrDeleteAsync(entitiesList);//Sync
context.BulkUpdate(entitiesList);                 context.BulkUpdateAsync(entitiesList);
context.BulkDelete(entitiesList);                 context.BulkDeleteAsync(entitiesList);
context.BulkRead(entitiesList);                   context.BulkReadAsync(entitiesList);
context.BulkSaveChanges();                        context.BulkSaveChangesAsync();
```

**-SQLite** requires package: [*SQLitePCLRaw.bundle_e_sqlite3*](https://docs.microsoft.com/en-us/dotnet/standard/data/sqlite/custom-versions?tabs=netcore-cli) with call to `SQLitePCL.Batteries.Init()`<br>

**Batch** Extensions are made on *IQueryable* DbSet and can be used as in the following code segment.<br>
They are done as pure sql and no check is done whether some are prior loaded in memory and are being Tracked.<br>
(*updateColumns* is optional param in which PropertyNames added explicitly when need update to it's default value)
```C#
// Delete
context.Items.Where(a => a.ItemId >  500).BatchDelete();
context.Items.Where(a => a.ItemId >  500).BatchDeleteAsync();

// Update (using Expression arg.) supports Increment/Decrement 
context.Items.Where(a => a.ItemId <= 500).BatchUpdate(a => new Item { Quantity = a.Quantity + 100 });
context.Items.Where(a => a.ItemId <= 500).BatchUpdateAsync(a => new Item { Quantity = a.Quantity + 100 });
  // can be as value '+100' or as variable '+incrementStep' (int incrementStep = 100;)
  
// Update (via simple object)
context.Items.Where(a => a.ItemId <= 500).BatchUpdate(new Item { Description = "Updated" });
context.Items.Where(a => a.ItemId <= 500).BatchUpdateAsync(new Item { Description = "Updated" });
// Update (via simple object) - requires additional Argument for setting to Property default value
var updateColumns = new List<string> { nameof(Item.Quantity) }; // Update 'Quantity' to default value('0'-zero)
var q = context.Items.Where(a => a.ItemId <= 500);
int affected = q.BatchUpdate(new Item { Description = "Updated" }, updateColumns); // result assigned to variable

// Truncate
context.Truncate<Entity>();
context.TruncateAsync<Entity>();
```
## Bulk info
If Windows Authentication is used then in ConnectionString there should be *Trusted_Connection=True;* because Sql credentials are required to stay in connection.<br>

When used directly each of these operations are separate transactions and are automatically committed.<br>
And if we need multiple operations in single procedure then explicit transaction should be used, for example:<br>
```C#
using (var transaction = context.Database.BeginTransaction())
{
    context.BulkInsert(entities1List);
    context.BulkInsert(entities2List);
    transaction.Commit();
}
```

**BulkInsertOrUpdate** method can be used when there is need for both operations but in one connection to database.<br>
It makes Update when PK(PrimaryKey) is matched, otherwise does Insert.<br>

**BulkInsertOrUpdateOrDelete** effectively [synchronizes](https://www.mssqltips.com/sqlservertip/1704/using-merge-in-sql-server-to-insert-update-and-delete-at-the-same-time/) table rows with input data.<br>
Those in Db that are not found in the list will be deleted.<br>
Partial Sync can be done on table subset using expression set on config with method:<br>
`bulkConfig.SetSynchronizeFilter<Item>(a => a.Quantity > 0);`<br>
Not supported for SQLite(Lite has only UPSERT statement) nor currently for PostgreSQL. Way to achieve there sync functionality is to Select or BulkRead existing data from DB, split list into sublists and call separately Bulk methods for BulkInsertOrUpdate and Delete.

**BulkRead** does SELECT and JOIN based on one or more Unique columns that are specified in Config `UpdateByProperties`.<br>
More info in the [Example](https://github.com/borisdj/EFCore.BulkExtensions#read-example) at the bottom.

**SaveChanges** uses Change Tracker to find all modified(CUD) entities and call proper BulkOperations for each table.<br>
Because it needs tracking it is slower then pure BulkOps but stil much faster then regular SaveChanges.<br>
With config *OnSaveChangesSetFK* setting FKs can be controled depending on whether PKs are generated in Db or in memory.<br>
Before calling this method newly created should be added into Range:
```C#
context.Items.AddRange(newEntities); // if newEntities is parent list it can have child sublists
context.BulkSaveChanges();
```
Practical general usage could be made in a way to override regular SaveChanges and if any list of Modified entities entries is greater then say 1000 to redirect to Bulk version.

Note: Bulk ops have optional argument *Type type* that can be set to type of Entity if list has dynamic runtime objects or is inhereted from Entity class.

## BulkConfig arguments

**Bulk** methods can have optional argument **BulkConfig** with properties (bool, int, object, List<string>):<br>
```C#
PROPERTY : DEFAULTvalue
-----------------------                           PropertiesToInclude: null,
PreserveInsertOrder: true,                        PropertiesToIncludeOnCompare: null,
SetOutputIdentity: false,	                  PropertiesToIncludeOnUpdate: null,
BatchSize: 2000,	                          PropertiesToExclude: null,
NotifyAfter: null,	                          PropertiesToExcludeOnCompare: null,
BulkCopyTimeout: null,	                          PropertiesToExcludeOnUpdate: null,
EnableStreaming: false,	                          UpdateByProperties: null,
UseTempDB: false,	                          EnableShadowProperties: false,
UniqueTableNameTempDb: true,	                  IncludeGraph: false,
CustomDestinationTableName: null,	          OmitClauseExistsExcept: false,
TrackingEntities: false,	                  DoNotUpdateIfTimeStampChanged: false,
WithHoldlock: true,	                          SRID: 4326,
CalculateStats: false,	                          DateTime2PrecisionForceRound: false,
SqlBulkCopyOptions: Default                       TemporalColumns: { "PeriodStart", "PeriodEnd" },
                                                  OnSaveChangesSetFK: true,  
--------------------------------------------------------------------------------------
METHOD: SetSynchronizeFilter<T>
```
If we want to change defaults, BulkConfig should be added explicitly with one or more bool properties set to true, and/or int props like **BatchSize** to different number.<br> Config also has DelegateFunc for setting *Underlying-Connection/Transaction*, e.g. in UnderlyingTest.<br>
When doing update we can chose to exclude one or more properties by adding their names into **PropertiesToExclude**, or if we need to update less then half column then **PropertiesToInclude** can be used. Setting both Lists are not allowed.

When using the **BulkInsert_/OrUpdate** methods, you may also specify the **PropertiesToIncludeOnCompare** and **PropertiesToExcludeOnCompare** properties. By adding a column name to the *PropertiesToExcludeOnCompare*, will allow it to be inserted and updated but will not update the row if any of the other columns in that row did not change. For example, if you are importing bulk data and want to keep an internal *CreateDate* or *UpdateDate*, you add those columns to the *PropertiesToExcludeOnCompare*.<br>
Another option that may be used in the same scenario are the **PropertiesToIncludeOnUpdate** and **PropertiesToExcludeOnUpdate** properties. These properties will allow you to specify insert-only columns such as *CreateDate* and *CreatedBy*.

If we want Insert only new and skip existing ones in Db (Insert_if_not_Exist) then use *BulkInsertOrUpdate* with config
`PropertiesToIncludeOnUpdate = new List<string> { "" }`

Additionaly there is **UpdateByProperties** for specifying custom properties, by which we want update to be done.<br>
When setting multiple props in UpdateByProps then match done by columns combined, like unique constrain based on those cols.<br>
Using UpdateByProperties while also having Identity column requires that Id property be [Excluded](https://github.com/borisdj/EFCore.BulkExtensions/issues/131).<br>
Also with PostgreSQL when matching is done it requires UniqueIndex so for custom UpdateByProperties that do not have un.ind., it is temporarily created in which case method can not be in transation (throws: *current transaction is aborted; CREATE INDEX CONCURRENTLY cannot run inside a transaction block*).<br>
If **NotifyAfter** is not set it will have same value as _BatchSize_ while **BulkCopyTimeout** when not set has SqlBulkCopy default which is 30 seconds and if set to 0 it indicates no limit.<br><br>
_SetOutputIdentity_ have purpose only when PK has Identity (usually *int* type with AutoIncrement), while if PK is Guid(sequential) created in Application there is no need for them.<br>
Also Tables with Composite Keys have no Identity column so no functionality for them in that case either.
```C#
var bulkConfig = new BulkConfig { SetOutputIdentity = true, BatchSize = 4000 };
context.BulkInsert(entList, bulkConfig);
context.BulkInsertOrUpdate(entList, new BulkConfig { SetOutputIdentity = true });
context.BulkInsertOrUpdate(entList, b => b.SetOutputIdentity = true); // example of BulkConfig set with Action arg.
```

**PreserveInsertOrder** is **true** by default and makes sure that entites are inserted to Db as ordered in entitiesList.<br>
When table has Identity column (int autoincrement) with 0 values in list they will temporary be automatically changed from 0s into range -N:-1.<br>
Or it can be manually set with proper values for order (Negative values used to skip conflict with existing ones in Db).<br>
Here single Id value itself doesn't matter, db will change it to next in sequence, what matters is their mutual relationship for sorting.<br>
Insertion order is implemented with [TOP](https://docs.microsoft.com/en-us/sql/t-sql/queries/top-transact-sql) in conjuction with ORDER BY. [stackoverflow:merge-into-insertion-order](https://stackoverflow.com/questions/884187/merge-into-insertion-order).<br>
This config should remain true when *SetOutputIdentity* is set to true on Entity containing NotMapped Property. [issues/76](https://github.com/borisdj/EFCore.BulkExtensions/issues/76)<br>
When using **SetOutputIdentity** Id values will be updated to new ones from database.<br>
With BulkInsertOrUpdate for those that will be updated it has to match with Id column, or other unique column(s) if using UpdateByProperties.<br>
For Sqlite combination of BulkInsertOrUpdate and IdentityId automatic set will not work properly since it does [not have full MERGE](https://github.com/borisdj/EFCore.BulkExtensions/issues/556) capabilities like SqlServer. Instead list can be split into 2 lists, and call separately BulkInsert and BulkUpdate.<br>
  
**SetOutputIdentity** is useful when BulkInsert is done to multiple related tables, that have Identity column.<br>
After Insert is done to first table, we need Id-s (if using Option 1) that were generated in Db because they are FK(ForeignKey) in second table.<br>
It is implemented with [OUTPUT](https://docs.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql) as part of MERGE Query, so in this case even the Insert is not done directly to TargetTable but to TempTable and then Merged with TargetTable.<br>
When used Id-s will be updated in entitiesList, and if *PreserveInsertOrder* is set to *false* then entitiesList will be cleared and reloaded.<br>
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

// Option 1
using (var transaction = context.Database.BeginTransaction())
{
    context.BulkInsert(entities, new BulkConfig { SetOutputIdentity = true });
    foreach (var entity in entities) {
        foreach (var subEntity in entity.ItemHistories) {
            subEntity.ItemId = entity.ItemId; // setting FK to match its linked PK that was generated in DB
        }
        subEntities.AddRange(entity.ItemHistories);
    }
    context.BulkInsert(subEntities);
    transaction.Commit();
}

// Option 2 using Graph (only for SQL Server) - all entities in relationship with main ones in list are BulkInsertUpdated
context.BulkInsert(entities, b => b.IncludeGraph = true);
  
// Option 3 with BulkSaveChanges() - uses ChangeTracker so little slower then direct Bulk
context.Items.AddRange(entities);
context.BulkSaveChanges();
```
When **CalculateStats** set to True the result returned in `BulkConfig.StatsInfo` (*StatsNumber-Inserted/Updated/Deleted*).<br>
If used for pure Insert (with Batching) then SetOutputIdentity should also be configured because Merge is required.<br>
**TrackingEntities** can be set to True if we want to have tracking of entities from BulkRead or if SetOutputIdentity is set.<br>
**UseTempDB** when set then BulkOperation has to be [inside Transaction](https://github.com/borisdj/EFCore.BulkExtensions/issues/49).<br>
**UniqueTableNameTempDb** when changed to false temp table name will be only 'Temp' without random numbers.<br>
**CustomDestinationTableName** can be set with 'TableName' only or with 'Schema.TableName'.<br>
**EnableShadowProperties** to add (normal) Shadow Property and persist value. Disables automatic discrimator, use manual method.<br>
**IncludeGraph** when set all entites that have relations with main ones from the list are also merged into theirs tables.<br>
**OmitClauseExistsExcept** removes the clause from Merge statement, useful when need to active triggers even for same data.<br>
_ Also in some [sql collation](https://github.com/borisdj/EFCore.BulkExtensions/issues/641) small and capital letters are considered  same (case-insensitive) so for BulkUpdate set it false.<br>
**DoNotUpdateIfTimeStampChanged** if set checks TimeStamp for Concurrency, ones with conflict will [not be updated](https://github.com/borisdj/EFCore.BulkExtensions/issues/469#issuecomment-803662721).<br>
**SRID** Spatial Reference Identifier - for SQL Server with NetTopologySuite.<br>
**DateTime2PrecisionForceRound** If dbtype datetime2 has precision less then default 7, example 'datetime2(3)' SqlBulkCopy does Floor instead of Round so when this Property is set then Rounding will be done in memory to make sure inserted values are same as with regular SaveChanges.<br>
**TemporalColumns** are shadow columns used for Temporal table. Default elements 'PeriodStart' and 'PeriodEnd' can be changed if those columns have custom names.<br>
**OnSaveChangesSetFK** is used only for BulkSaveChanges. When multiply entries have FK relationship which is Db generated, this set proper value after reading parent PK from Db. IF PK are generated in memory like are some Guid then this can be set to false for better efficiency.
  
**SqlBulkCopyOptions** is Enum (only for SqlServer) with [[Flags]](https://stackoverflow.com/questions/8447/what-does-the-flags-enum-attribute-mean-in-c) attribute which enables specifying one or more options:<br>
*Default, KeepIdentity, CheckConstraints, TableLock, KeepNulls, FireTriggers, UseInternalTransaction*<br>
If need to set Identity PK in memory, Not let DB do the autoincrement, then need to use **KeepIdentity**:<br>
`var bulkConfig = new BulkConfig { SqlBulkCopyOptions = SqlBulkCopyOptions.KeepIdentity };`<br>
Useful for example when copying from one Db to another.

**SetSynchronizeFilter<T>** A method that receives and sets expresion filter on entities to delete when using BulkInsertOrUpdateOrDelete.<br>

Last optional argument is **Action progress** (Example in *EfOperationTest.cs* *RunInsert()* with *WriteProgress()*).
```C#
context.BulkInsert(entitiesList, null, (a) => WriteProgress(a));
```

Library supports [Global Query Filters](https://docs.microsoft.com/en-us/ef/core/querying/filters) and [Value Conversions](https://docs.microsoft.com/en-us/ef/core/modeling/value-conversions) as well.</br>
Additionally BatchUpdate and named Property works with [EnumToString Conversion](https://github.com/borisdj/EFCore.BulkExtensions/issues/397).</br>
It can map [OwnedTypes](https://docs.microsoft.com/en-us/ef/core/modeling/owned-entities), also next are links with info how to achieve 
[NestedOwnedTypes](https://github.com/borisdj/EFCore.BulkExtensions/issues/167#issuecomment-476737959) and 
[OwnedInSeparateTable](https://github.com/borisdj/EFCore.BulkExtensions/issues/114#issuecomment-803462928).</br>
Table splitting are somewhat specific but could be configured in way [Set TableSplit](https://github.com/borisdj/EFCore.BulkExtensions/issues/352#issuecomment-803674404).</br>
With [Computed](https://docs.microsoft.com/en-us/ef/core/modeling/relational/computed-columns) and [Timestamp](https://docs.microsoft.com/en-us/ef/core/modeling/concurrency) Columns it will work in a way that they are automatically excluded from Insert. And when combined with *SetOutputIdentity* they will be Selected.<br>
[Spatial](https://docs.microsoft.com/en-us/sql/relational-databases/spatial/spatial-data-types-overview?view=sql-server-ver15) types, like Geometry, also supported and if  Entity has one, clause *EXIST ... EXCEPT* is skipped because it's not comparable.<br>
Performance for bulk ops measured with `ActivitySources` named: '*BulkExecute*' (tags: '*operationType*', '*entitiesCount*')<br>
Bulk Extension methods can be [Overridden](https://github.com/borisdj/EFCore.BulkExtensions/issues/56) if required, for example to set AuditInfo.<br>
If having problems with Deadlock there is useful info in [issue/46](https://github.com/borisdj/EFCore.BulkExtensions/issues/46).

## TPH inheritance

When having TPH ([Table-Per-Hierarchy](https://docs.microsoft.com/en-us/aspnet/core/data/ef-mvc/inheritance)) inheritance model it can be set in 2 ways.<br>
First is automatically by Convention in which case Discriminator column is not directly in Entity but is [Shadow](http://www.learnentityframeworkcore.com/model/shadow-properties) Property.<br>
And second is to explicitly define Discriminator property in Entity and configure it with `.HasDiscriminator()`.<br>
Important remark regarding the first case is that since we can not set directly Discriminator to certain value we need first to add list of entities to DbSet where it will be set and after that we can call Bulk operation. Note that SaveChanges are not called and we could optionally turn off TrackingChanges for performance. Example:
```C#
public class Student : Person { ... }
context.Students.AddRange(entities); // adding to Context so that Shadow property 'Discriminator' gets set
context.BulkInsert(entities);
```
**TPT** (Table-Per-Type) as of v5 is [partially supported](https://github.com/borisdj/EFCore.BulkExtensions/issues/493).

## Read example

When we need to Select from big List of some Unique Prop./Column use BulkRead (JOIN done in Sql) for Efficiency:<br>
```C#
// instead of WhereIN which will TimeOut for List with several thousand records
var entities = context.Items.Where(a => itemsNames.Contains(a.Name)).AsNoTracking().ToList(); //SQL IN operator
// or JOIN in Memory that loads entire table
var entities = context.Items.Join(itemsNames, a => a.Name, p => p, (a, p) => a).AsNoTracking().ToList();

// USE
var items = itemsNames.Select(a => new Item { Name = a }).ToList(); // creating list of Items where only Name is set
var bulkConfig = new BulkConfig { UpdateByProperties = new List<string> { nameof(Item.Name) } };
context.BulkRead(items, bulkConfig); // Items list will be loaded from Db with data(other properties)
```

## Performances

Following are performances (in seconds)
* For SQL Server (v. 2019):

| Ops\Rows | EF 100K | Bulk 100K | EF 1 MIL.| Bulk 1 MIL.|
| -------- | ------: | --------: | -------: | ---------: |
| Insert   |  11 s   | 3 s       |   60 s   | 15  s      |
| Update   |   8 s   | 4 s       |   84 s   | 27  s      |
| Delete   |  50 s   | 3 s       | 5340 s   | 15  s      |

TestTable has 6 columns (Guid, string, string, int, decimal?, DateTime).<br>
All were inserted and 2 of them (string, DateTime) were updated.<br>
Test done locally on configuration: INTEL i7-10510U CPU 2.30GHz, DDR3 16 GB, SSD SAMSUNG MZ 512 GB.<br>
For small data sets there is an overhead since most Bulk ops need to create Temp table and also Drop it after finish.<br>
_Probably good advice would be to use Bulk ops for sets greater than 1000.
