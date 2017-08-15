# EFCore.BulkExtensions
EntityFrameworkCore extensions for Bulk operations (**Insert, Update, Delete**).<br>
Library is targeting *NetStandard 1.4* so it can used on project targeting both *NetCore(1.0+)* or *NetFramework(4.6.1+)*.<br>
It is Lightweight and very Efficient, having all mostly used CUD operation.<br>
Under the hood uses [SqlBulkCopy](https://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqlbulkcopy.aspx) for Insert, for Update/Delete combines BulkInsert with raw Sql ['MERGE'](https://docs.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql) (MsSQL 2008+).

Available on [NuGet](https://www.nuget.org/packages/EFCore.BulkExtensions/). Latest Version: 1.0.7<br>
Package manager console command for installation: *Install-Package EFCore.BulkExtensions*

Usage is simple and pretty straightforward.<br>
Extensions are made on *DbContext* class and can be used like this (both regular and Async methods are supported):
```csharp
context.BulkInsert(entitiesList);                context.BulkInsertAsync(entitiesList);
context.BulkUpdate(entitiesList);                context.BulkUpdateAsync(entitiesList);
context.BulkDelete(entitiesList);                context.BulkDeleteAsync(entitiesList);
context.BulkInsertOrUpdate(entitiesList);        context.BulkInsertOrUpdateAsync(entitiesList);
```
In ConnectionString you should have *Trusted_Connection=True;* because Sql credentials are required to stay in connection.<br>

Each of these operations are separate transactions.<br>
So when using multiple operations in single procedure and if one would break because of some Db constraint, previous would stay executed.<br>
In scenario where All or Nothing is required, there should be additional logic with try/catch block, catch having methods that would revert previously executed operations.

## BulkConfig arguments

**BulkInsertOrUpdate** method can be used when there is need for both operations but in one connection to database.<br>
It makes Update when PK is matched, otherwise does Insert.<br>

Additionally **BulkInsert** and **BulkInsertOrUpdate** methods can have optional argument **BulkConfig** with bool properties:<br>
`{ PreserveInsertOrder, SetOutputIdentity, BatchSize }`.<br>
Default behaviour is { false, false, 2000 } and if we want to change it, BulkConfig should be added explicitly with one or both bool properties set to true, and/or **BatchSize** to different number.<br>
This argument has purpose only when PK has Identity (usually *int* type with AutoIncrement), while if PK is Guid(sequential) created in Application there is no need for it. Also Tables with Composite Keys hava no Identity column so no function for SetOutputIdentity in that case either.
```csharp
context.BulkInsert(entitiesList, new BulkConfig { PreserveInsertOrder = true, SetOutputIdentity = true, BatchSize = 5000});
context.BulkInsertOrUpdate(entitiesList, new BulkConfig { PreserveInsertOrder = true });
```
**PreserveInsertOrder** makes sure that entites are inserted to Db as they are ordered in entitiesList.<br>
However for this to work Id column needs to be set for the proper order.<br>
For example if table already has rows, let's say it has 1000 rows with Id-s (1:1000), and we now want to add 300 more.<br>
Since Id-s are generated in Db we could not set them, they would all be 0 (int default) in list.<br>
But if we want to keep the order as they are ordered in list then those Id-s should be set say 1 to 300.<br>
Here single Id value itself doesn't matter, db will change it to (1001:1300), what matters is their mutual relationship for sorting.<br>
Insertion order is implemented with [TOP](https://docs.microsoft.com/en-us/sql/t-sql/queries/top-transact-sql) in conjuction with ORDER BY. [stackoverflow:merge-into-insertion-order](https://stackoverflow.com/questions/884187/merge-into-insertion-order).

When using **PreserveInsertOrder** with **BulkInsertOrUpdate** method, Id value does matter for those that will be updated.<br>
If we need to sort those for insert and not have conflict with existing Id-s, there are 2 ways:<br>
One is set Id to really high values, order of magnitude 10^10, and another even better setting them to negative values.<br>
So if we have list of 8000, say 3000 for update (they keep the real Id) and 5000 for insert then Id-s could be (-5000:-1).

**SetOutputIdentity** is useful when BulkInsert is done to multiple related tables, that have Identity column.<br>
So after Insert is done to first table, we need Id-s that were generated in Db becasue they are FK in second table.<br>
It is implemented with [OUTPUT](https://docs.microsoft.com/en-us/sql/t-sql/queries/output-clause-transact-sql) as part of MERGE Query, so in this case even the Insert is not done directly to TargetTable but to TempTable and then Merged with TargetTable.<br>
When used if *PreserveInsertOrder* is also set to *true* Id-s will be updated in entitiesList, and if *PreserveInsertOrder* is *false* then entitiesList will be cleared and reloaded.

When having TPH ([Table-Per-Hierarchy](https://docs.microsoft.com/en-us/aspnet/core/data/ef-mvc/inheritance)) inheritance model it can be set in 2 ways.<br>
First automatically by Convention in which case Discriminator column is not directly in Entity but is [Shadow](http://www.learnentityframeworkcore.com/model/shadow-properties) Property.<br>
And second is to explicitly define Discriminator property in Entity and configure it with `.HasDiscriminator()`.<br>
Important remark regarding the first case is that since we can not set directly Discriminator to certain value we need first to add list of entities to DbSet where it will be set and after that we can call Bulk operation. Note that SaveChanges are not called and we could optionally turn of TrackingChanges for performance. Example:
```csharp
public class Student : Person { ...
context.Students.AddRange(entities); // adding to Context so that Shadow property 'Discriminator' gets set
context.BulkInsert(entities);
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

TestTable has 6 column(Guid, string, string, int, decimal?, DateTime).<br>
All were inserted and 2 of them (string, DateTime) were updated.<br>
Test was done locally on following configuration: INTEL Core i5-3570K 3.40GHz, DDRIII 8GB x 2, SSD 840 EVO 128 GB.

If you find this project useful you can mark it by leaving a Github Star.

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on code of conduct, and the process for submitting pull requests.

[![NuGet](https://img.shields.io/npm/l/express.svg)](https://github.com/borisdj/EFCore.BulkExtensions/blob/master/LICENSE)

