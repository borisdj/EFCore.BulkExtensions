using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.BulkExtensions.Tests.IncludeGraph;

public class Issue1337
{
    [Fact]
    public async Task Issue1337Test()
    {
        using var context = new MyDbContext(ContextUtil.GetOptions<MyDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Issue1337"));

        context.Database.EnsureDeleted();
        context.Database.EnsureCreated();

        var bulkConfig = new BulkConfig { IncludeGraph = true, CalculateStats = true };

        Cust[] entities = [
            new()
            {
                CustId = "test1",
                ContactMethods = new()
                {
                    HomePhone = "homephone1",
                    EmailAdresses = [
                        new Email() { Address = "email1", Type = "emailtype1" },
                        new Email() { Address = "email2", Type = "emailtype2" },
                    ]
                },
            }
        ];

        // Without any changes, this crashes with System.InvalidOperationException : Column 'ContactMethodsCustomerId' does not allow DBNull.Value.

        // With the changes in this PR until now, the merge of the emails works, but now I get an error when trying to read the output of that operation:
        // System.InvalidOperationException : An exception was thrown while attempting to evaluate a LINQ query parameter expression. See the inner exception for more information.
        // ----System.InvalidOperationException : Cannot create a DbSet for 'Email' because it is configured as an owned entity type and must be accessed through its owning entity type 'ContactMethods'.See https://aka.ms/efcore-docs-owned for more information.

        await context.BulkInsertOrUpdateAsync(entities, bulkConfig);
    }
}

public class Cust
{
    public string CustId { get; set; } = default!;
    public ContactMethods ContactMethods { get; set; } = default!;
}

public class ContactMethods
{
    public string HomePhone { get; set; } = default!;
    public ICollection<Email> EmailAdresses { get; set; } = default!;
}

public class Email
{
    public string Address { get; set; } = default!;
    public string Type { get; set; } = default!;
}

public class MyDbContext(DbContextOptions opts) : DbContext(opts)
{
    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<Cust>(c =>
            c.OwnsOne(c => c.ContactMethods, cm => cm.OwnsMany(y => y.EmailAdresses)));
    }
}
