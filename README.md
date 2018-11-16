# EFCore.BulkExtensions
EntityFrameworkCore extensions: Bulk operations (**Insert, Update, Delete, Read, Upsert, Sync**) and Batch (**Delete, Update**).<br>
Library is Lightweight and very Efficient, having all mostly used CRUD operation.<br>
Was selected in top 20 [EF Core Extensions](https://docs.microsoft.com/en-us/ef/core/extensions/) recommended by Microsoft.<br>
It is targeting NetStandard 2.0 so it can be used on project targeting NetCore(2.0+) or NetFramework(4.6.1+).<br>
Current version is using EF Core 2.1. and at the moment supports ONLY MsSQL(2008+).<br>
For EF Core 2.0 install 2.0.8 Nuget, and for EF Core 1.x use 1.1.0 (targeting NetStandard 1.4)<br>
Under the hood uses [SqlBulkCopy](https://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqlbulkcopy.aspx) for Insert, for Update/Delete combines BulkInsert with raw Sql [MERGE](https://docs.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql).

Available on [![NuGet](https://img.shields.io/badge/NuGet-2.2.5-blue.svg)](https://www.nuget.org/packages/EFCore.BulkExtensions/) latest version.<br>
Package manager console command for installation: *Install-Package EFCore.BulkExtensions*

## Contributing

If you find this project useful you can mark it by leaving a Github **\*Star**.</br>
If you would like to support the Project by making a Donation ($10) #BuyMeBeer, you are welcome to do so:<br>
[![Donate](https://img.shields.io/badge/donate-PayPal-yellow.svg)](https://www.paypal.me/BorisDjurdjevic/10) or
[![Donate Bitcoin](https://img.shields.io/badge/donate-Bitcoin-orange.svg)](https://borisdj.github.io/pages/donateBTC.html)<br>
Please read [CONTRIBUTING](CONTRIBUTING.md) for details on code of conduct, and the process for submitting pull requests.<br>
[![NuGet](https://img.shields.io/npm/l/express.svg)](https://github.com/borisdj/EFCore.BulkExtensions/blob/master/LICENSE)

Want to **Contact** us for Hire (Development & Consulting): [www.codis.tech](http://www.codis.tech)

## Usage
It's pretty simple and straightforward.<br>
**Bulk** Extensions are made on *DbContext* class and can be used like this (both regular and Async methods are supported):
```C#
context.BulkInsert(entitiesList);                 context.BulkInsertAsync(entitiesList);
context.BulkUpdate(entitiesList);                 context.BulkUpdateAsync(entitiesList);
context.BulkDelete(entitiesList);                 context.BulkDeleteAsync(entitiesList);
context.BulkInsertOrUpdate(entitiesList);         context.BulkInsertOrUpdateAsync(entitiesList);         // Upsert
context.BulkInsertOrUpdateOrDelete(entitiesList); context.BulkInsertOrUpdateOrDeleteAsync(entitiesList); // Sync
context.BulkRead(entitiesList);                   context.BulkReadAsync(entitiesList);
```
**Batch** Extensions are made on *IQueryable* DbSet and can be used as in the following code segment.<br>
They are done as pure sql and no check is done whether some are prior loaded in memory and are being Tracked.
(*updateColumns* optional parameter in which PropertyName is added explicitly when we need update to it's default value)
```C#
context.Items.Where(a => a.ItemId <= 500).BatchUpdate(new Item { Description = "Updated" });
context.Items.Where(a => a.ItemId <= 500).BatchUpdateAsync(new Item { Description = "Updated" }, updateColumns);
context.Items.Where(a => a.ItemId >  500).BatchDelete();
context.Items.Where(a => a.ItemId >  500).BatchDeleteAsync();
```
## Bulk info
If Windows Authentication is used then in ConnectionString there should be *Trusted_Connection=True;* because Sql credentials are required to stay in connection.<br>

When used directly each of these operations are separate transactions and are automatically committed.<br>
And if we need multiple operations in single procedure then explicit transaction should be used, for example:
```C#
using (var transaction = context.Database.BeginTransaction())
{
    context.BulkInsert(entitiesList);
    context.BulkInsert(subEntitiesList);
    transaction.Commit();
}
```

**BulkInsertOrUpdate** method can be used when there is need for both operations but in one connection to database.<br>
It makes Update when PK(PrimaryKey) is matched, otherwise does Insert.<br>

**BulkInsertOrUpdateOrDelete** effectively [synchronizes](https://www.mssqltips.com/sqlservertip/1704/using-merge-in-sql-server-to-insert-update-and-delete-at-the-same-time/) table rows with input data.<br>
Those in Db that are not found in the list will be deleted.<br>

**BulkRead** does SELECT and JOIN based on one or more Unique columns that are specified in Config `UpdateByProperties`.<br>
More info in the Example at the bottom.<br>

## BulkConfig arguments

**BulkInsert** and **BulkInsertOrUpdate** methods can have optional argument **BulkConfig** with properties (bool, int, object, List<string>):<br>
*{ PreserveInsertOrder, SetOutputIdentity, BatchSize, NotifyAfter, BulkCopyTimeout, EnableStreaming, UseTempDB, WithHoldlock, TrackingEntities, CalculateStats, StatsInfo, PropertiesToInclude, PropertiesToExclude, UpdateByProperties, SqlBulkCopyOptions }*<br>
Default behaviour is { false, false, 2000,  null, null, false, false, true, false, false, null, null, null, null, Default } and if we want to change it, BulkConfig should be added explicitly with one or more bool properties set to true, and/or int props like **BatchSize** to different number.<br>
When doing update we can chose to exclude one or more properties by adding their names into **PropertiesToExclude**, or if we need to update less then half column then **PropertiesToInclude** can be used. Setting both Lists are not allowed. Additionaly there is **UpdateByProperties** that allows specifying custom properties, besides PK, by which we want update to be done.<br>
If **NotifyAfter** is not set it will have same value as _BatchSize_ while **BulkCopyTimeout** when not set has SqlBulkCopy default which is 30 seconds and if set to 0 it indicates no limit.<br>
_PreserveInsertOrder_ and _SetOutputIdentity_ have purpose only when PK has Identity (usually *int* type with AutoIncrement), while if PK is Guid(sequential) created in Application there is no need for them. Also Tables with Composite Keys have no Identity column so no functionality for them in that case either.
```C#
context.BulkInsert(entitiesList, new BulkConfig { PreserveInsertOrder = true, SetOutputIdentity = true, BatchSize = 4000 });
context.BulkInsertOrUpdate(entitiesList, new BulkConfig { PreserveInsertOrder = true });
```

**PreserveInsertOrder** makes sure that entites are inserted to Db as they are ordered in entitiesList.<br>
However for this to work Id column needs to be set for the proper order.<br>
For example if table already has rows, let's say it has 1000 rows with Id-s (1:1000), and we now want to add 300 more.<br>
Since Id-s are generated in Db we could not set them, they would all be 0 (int default) in list.<br>
But if we want to keep the order as they are ordered in list then those Id-s should be set say 1 to 300.<br>
Here single Id value itself doesn't matter, db will change it to (1001:1300), what matters is their mutual relationship for sorting.<br>
Insertion order is implemented with [TOP](https://docs.microsoft.com/en-us/sql/t-sql/queries/top-transact-sql) in conjuction with ORDER BY. [stackoverflow:merge-into-insertion-order](https://stackoverflow.com/questions/884187/merge-into-insertion-order).<br>
This config should also be used when we have set *SetOutputIdentity* on Entity containing NotMapped Property. [issues/76](https://github.com/borisdj/EFCore.BulkExtensions/issues/76)

When using **PreserveInsertOrder** with **SetOutputIdentity** Id value does matter.<br>
If it's BulkInsertOrUpdate method for those that will be updated it has to match Id.<br>
And if we need to sort those for insert(BulkInsert/OrUpdate) and not have conflict with existing Id-s, there are 2 ways:<br>
One is set Id to really high values, order of magnitude 10^10, and another even better setting them to negative values.<br>
So if we have list of 8000, say 3000 for update (they keep the real Id) and 5000 for insert then Id-s could be (-5000:-1).

**SetOutputIdentity** is useful when BulkInsert is done to multiple related tables, that have Identity column.<br>
After Insert is done to first table, we need Id-s that were generated in Db because they are FK(ForeignKey) in second table.<br>
It is implemented with [OUTPUT](https://docs.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql) as part of MERGE Query, so in this case even the Insert is not done directly to TargetTable but to TempTable and then Merged with TargetTable.<br>
When used if *PreserveInsertOrder* is also set to *true* Id-s will be updated in entitiesList, and if *PreserveInsertOrder* is *false* then entitiesList will be cleared and reloaded.<br>
Example of *SetOutputIdentity* with parent-child FK related tables:
```C#
int numberOfEntites = 1000;
var entities = new List<Item>(); var subEntities = new List<ItemHistory>();
for (int i = 1; i <= numberOfEntites; i++)
{
    var entity = new Item { Name = $"Name {i}", ItemHistories = new List<ItemHistory>() }; // ItemId set by Db
    entity.ItemHistories.Add(new ItemHistory { Remark = $"Info {i}.1" });
    entity.ItemHistories.Add(new ItemHistory { Remark = $"Info {i}.2" });
}
using (var transaction = context.Database.BeginTransaction()) {
    context.BulkInsert(entities, new BulkConfig { PreserveInsertOrder = true, SetOutputIdentity = true });
    foreach (var entity in entities) {
        foreach (var subEntity in entity.ItemHistories) {
            subEntity.ItemId = entity.ItemId; // setting FK to match its linked PK that was generated in DB
        }
        subEntities.AddRange(entity.ItemHistories);
    }
    context.BulkInsert(subEntities);
    transaction.Commit();
}
```
When **CalculateStats** is set to True the result is return in `BulkConfig.StatsInfo` (*StatsNumber-Inserted/Updated*).
**TrackingEntities** can be set to True if we want to have tracking of entities from BulkRead or when SetOutputIdentity is set.

**UseTempDB** when set then BulkOperation has to be [inside Transaction](https://github.com/borisdj/EFCore.BulkExtensions/issues/49)

**SqlBulkCopyOptions** is Enum with [[Flags]](https://stackoverflow.com/questions/8447/what-does-the-flags-enum-attribute-mean-in-c) attribute which enables specifying one or more options:<br>
*Default, KeepIdentity, CheckConstraints, TableLock, KeepNulls, FireTriggers, UseInternalTransaction*

Last optional argument is **Action progress** (Example in *EfOperationTest.cs* *RunInsert()* with *WriteProgress()*).
```C#
context.BulkInsert(entitiesList, null, (a) => WriteProgress(a));
```

Library supports [Global Query Filters](https://docs.microsoft.com/en-us/ef/core/querying/filters) and [Value Conversions](https://docs.microsoft.com/en-us/ef/core/modeling/value-conversions) as well.</br>
It also maps [OwnedTypes](https://docs.microsoft.com/en-us/ef/core/modeling/owned-entities), which is implemented with `DataTable` class.</br>
With [Computed](https://docs.microsoft.com/en-us/ef/core/modeling/relational/computed-columns) and [Timestamp](https://docs.microsoft.com/en-us/ef/core/modeling/concurrency) Columns it will work in a way that they are automatically excluded from Insert. And when combined with *SetOutputIdentity* they will be Selected.<br>
Bulk Extension methods can be [Overridden](https://github.com/borisdj/EFCore.BulkExtensions/issues/56) if required, for example to set AuditInfo.<br>
For mapping FKs explicit Id properties have to be in entity. Having only [Navigation](https://github.com/borisdj/EFCore.BulkExtensions/issues/95) property is not supported.<br>
If having problems with Deadlock there is useful info in [issue/46](https://github.com/borisdj/EFCore.BulkExtensions/issues/46).

## TPH inheritance

When having TPH ([Table-Per-Hierarchy](https://docs.microsoft.com/en-us/aspnet/core/data/ef-mvc/inheritance)) inheritance model it can be set in 2 ways.<br>
First is automatically by Convention in which case Discriminator column is not directly in Entity but is [Shadow](http://www.learnentityframeworkcore.com/model/shadow-properties) Property.<br>
And second is to explicitly define Discriminator property in Entity and configure it with `.HasDiscriminator()`.<br>
Important remark regarding the first case is that since we can not set directly Discriminator to certain value we need first to add list of entities to DbSet where it will be set and after that we can call Bulk operation. Note that SaveChanges are not called and we could optionally turn of TrackingChanges for performance. Example:
```C#
public class Student : Person { ... }
context.Students.AddRange(entities); // adding to Context so that Shadow property 'Discriminator' gets set
context.BulkInsert(entities);
```

## Read example

When we need to Select from big List of some Unique Prop./Column instead of `Join` or `Contains` use BulkRead for [Efficiency](https://stackoverflow.com/questions/16824510/select-multiple-records-based-on-list-of-ids-with-linq):<br>
```C#
// instead of
var entities = context.Items.Join(itemsNames, a => a.Name, p => p, (a, p) => a).AsNoTracking().ToList(); // or
var entities = context.Items.Where(a => itemsNames.Contains(a.Name)).AsNoTracking().ToList();
// use
var items = itemsNames.Select(a => new Item { Name = a });
context.Items.BulkRead(items, new BulkConfig { UpdateByProperties = new List<string> { nameof(Item.Name) }) // items loaded with data
```

## Performances

Following are performances (in seconds):

| Operations\Rows | 100,000 EF | 100,000 EFBulk | 1,000,000 EFBulk |
| --------------- | ---------: | -------------: | ---------------: |
| Insert          |  38.98 s   | 2.10 s         | 17.99 s          |
| Update          | 109.25 s   | 3.96 s         | 31.45 s          |
| Delete          |   7.26 s   | 2.04 s         | 12.18 s          |
|-----------------|------------|----------------|------------------|
| **Together**    |  70.70 s   | 5.88 s         | 56.84 s          |

TestTable has 6 columns (Guid, string, string, int, decimal?, DateTime).<br>
All were inserted and 2 of them (string, DateTime) were updated.<br>
Test was done locally on following configuration: INTEL Core i5-3570K 3.40GHz, DDRIII 8GB x 2, SSD 840 EVO 128 GB.

