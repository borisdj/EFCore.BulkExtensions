
[![NuGet](https://img.shields.io/nuget/v/EFCore.BulkExtensions.svg)](https://www.nuget.org/packages/EFCore.BulkExtensions)

# EFCore.BulkExtensions
EntityFrameworkCore extensions for Bulk operations (**Insert, Update, Delete**).<br>
Library is targeting *NetStandard 1.4* so it can used on project targeting both *NetCore(1.0+)* or *NetFramework(4.6.1+)*.<br>
It is Lightweight and very Efficient, having all mostly used CUD operation.<br>
Under the hood used *SqlBulkCopy* for Insert, for Update/Delete combines BulkInsert with raw Sql *'MERGE'* (MsSQL 2008+).

Avalaible on [NuGet](https://www.nuget.org/packages/EFCore.BulkExtensions/).

Usage is simple and pretty straightforward.
Extensions are made on *DbContext* class and can be used like this:
```csharp
context.BulkInsert(entitiesList);
context.BulkInsertOrUpdate(entitiesList);
context.BulkUpdate(entitiesList);
context.BulkDelete(entitiesList);
```

Additionally it has `InsertOrUpdate` method when there is need for both operation but in one connection to database.<br>
However this only works with tables that do not have Identity column, because of *IDENTITY_INSERT* settings.<br>
Identity column are usually int type with *AutoIncrement*.<br>
Usecase when this works is with *PK* that do not have Db Identity but Id value is created in application like *Guid* usually.

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
Test was done locally on following configuration: INTEL Core i5-3570K, DDRIII 8GB x 2, HDD Seagate 1TB, SSD 128 GB.
