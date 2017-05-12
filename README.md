# EFCore.BulkExtensions
EntityFrameworkCore extensions for Bulk operations (Insert, Update, Delete).
Library is targeting *NetStandard 1.4* so it can used on project targeting both *NetCore(1.0+)* or N*etFramework(4.6.1+)*.
It is Lightweight and very Efficient, having all mostly used CUD operation.

Usage is simple and pretty straightforward.
Extension are made on *DbContext* class and can be used like this:
```csharp
context.BulkInsert(entitiesList);
context.BulkInsertOrUpdate(entitiesList);
context.BulkUpdate(entitiesList);
context.BulkDelete(entitiesList);
```

Additionally it has `InsertOrUpdate` method when there is need for both operation but in one connection to database.
However this only works with tables that do not have Identity column, because of *IDENTITY_INSERT* settings.
Identity column are usually int type with *AutoIncrement*.
Usecase when this works is with *PK* that do not have Db Identity but Id value is created in application like *Guid* usually.

Following are performances(in seconds):

| Operations\Rows | 100,000 EF | 100,000 EFBulk | 1,000,000 EFBulk |
| --------------- | ---------: | -------------: | ---------------: |
|Insert           |  38.98 s   | 2.10 s         | 17.99 s          |
|Update           | 109.25 s   | 3.96 s         | 31.45 s          |
|Delete           |  7.26  s   | 2.04 s         | 12.18 s          |
|-----------------|------------|----------------|------------------|
|**Together**     |  70.70 s   | 5.88 s         | 56.84 s          |


TestTable has 6 column(Guid, string, string, int, decimal?, DateTime), all were inserted and 2 of them(string, DateTime) were updated.
