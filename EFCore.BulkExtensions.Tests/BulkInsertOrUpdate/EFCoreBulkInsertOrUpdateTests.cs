using Xunit;
using Xunit.Abstractions;

#nullable disable
namespace EFCore.BulkExtensions.Tests.BulkInsertOrUpdate;

public class EfCoreBulkInsertOrUpdateTests : IClassFixture<EfCoreBulkInsertOrUpdateTests.DatabaseFixture>
{
    private readonly ITestOutputHelper _writer;
    private readonly DatabaseFixture _dbFixture;

    public EfCoreBulkInsertOrUpdateTests(ITestOutputHelper writer, DatabaseFixture dbFixture)
    {
        _writer = writer;
        _dbFixture = dbFixture;
    }
    
    
    
    public class DatabaseFixture : BulkDbTestsFixture
    {
        public DatabaseFixture() : base(nameof(EfCoreBulkInsertOrUpdateTests))
        {
        }
    }
}
