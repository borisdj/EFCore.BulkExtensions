# EFCore.BulkExtensions
EntityFrameworkCore extensions for Bulk operations (**Insert, Update, Delete**).<br>
Library is targeting *NetStandard 1.4* so it can used on project targeting both *NetCore(1.0+)* or *NetFramework(4.6.1+)*.<br>
It is Lightweight and very Efficient, having all mostly used CUD operation.<br>
Under the hood uses [SqlBulkCopy](https://msdn.microsoft.com/en-us/library/system.data.sqlclient.sqlbulkcopy.aspx) for Insert, for Update/Delete combines BulkInsert with raw Sql ['MERGE'](https://docs.microsoft.com/en-us/sql/t-sql/statements/merge-transact-sql) (MsSQL 2008+).

Available on [NuGet](https://www.nuget.org/packages/EFCore.BulkExtensions/). Latest Version: 1.0.2<br>
Package manager console command for installation: *Install-Package EFCore.BulkExtensions*

Usage is simple and pretty straightforward.
Extensions are made on *DbContext* class and can be used like this:
```csharp
context.BulkInsert(entitiesList);
context.BulkUpdate(entitiesList);
context.BulkDelete(entitiesList);
context.BulkInsertOrUpdate(entitiesList);
```

Each of these operations are separate transactions.<br>
So when using multiple operations in single procedure and if, for example, second would break because of some Db constraint, the first one would stay executed.<br>
In scenario where All or Nothing is required, there should be additional logic with try/catch block, catch having methods that would revert previously executed operations.

Additionally library has **InsertOrUpdate** method when there is need for both operations but in one connection to database.<br>
However this only works with tables that do not have Identity column, because of *IDENTITY_INSERT* settings.<br>
Identity column are usually int type with *AutoIncrement*.<br>
Use case when this works is with *PK* that do not have Db Identity but Id value is created in application like *GUIDs* are usually.

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

[![NuGet](https://img.shields.io/npm/l/express.svg)](https://github.com/borisdj/EFCore.BulkExtensions/blob/master/LICENSE)

