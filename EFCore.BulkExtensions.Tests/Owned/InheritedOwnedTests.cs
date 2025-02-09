using System;
using System.ComponentModel.DataAnnotations;
using System.Threading.Tasks;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EFCore.BulkExtensions.Tests.Owned;

public class InheritedOwnedTests
{
    [Theory]
    [InlineData(SqlType.SqlServer)]
    public async Task InheritedOwnedTest(SqlType sqlType)
    {
        var options = new ContextUtil(sqlType)
            .GetOptions<InheritedDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_NestedOwned");
        using var context = new InheritedDbContext(options);

        await context.Database.EnsureDeletedAsync();
        await context.Database.EnsureCreatedAsync();

        await context.BulkInsertOrUpdateAsync([
            new CheckingAccount
            {
                AccountId = Guid.NewGuid(),
                EntityInfo = new EntityInfo
                {
                    Key = "NZ4321"
                },
                Overdraft = new Overdraft
                {
                    Limit = 5000
                }
            }
        ]);
        await context.BulkInsertOrUpdateAsync([
            new SavingsAccount
            {
                AccountId = Guid.NewGuid(),
                EntityInfo = new EntityInfo
                {
                    Key = "AZ1234",
                },
                InterestRate = new InterestRate
                {
                    Rate = 6.9m
                },
            }
        ]);
        await context.BulkInsertOrUpdateAsync([
            new Client
            {
                ClientId = Guid.NewGuid(),
                Name = "Mike Mao",
                EntityInfo = new EntityInfo
                {
                    Key = "MM123",
                }
            }
        ]);

        var checkingAccount = await context.CheckingAccounts.SingleAsync();
        Assert.Equal("NZ4321", checkingAccount.EntityInfo.Key);
        Assert.Equal(5000, checkingAccount.Overdraft.Limit);

        var savingsAccount = await context.SavingsAccounts.SingleAsync();
        Assert.Equal("AZ1234", savingsAccount.EntityInfo.Key);
        Assert.Equal(6.9m, savingsAccount.InterestRate.Rate);
        
        var client = await context.Clients.SingleAsync();
        Assert.Equal("MM123", client.EntityInfo.Key);
        Assert.Equal("Mike Mao", client.Name);
    }
}

public record Client
{
    [Key] public Guid ClientId { get; init; }
    public string Name { get; init; } = default!;
    public EntityInfo EntityInfo { get; init; } = default!;
}

public abstract record AbstractAccount
{
    [Key] public Guid AccountId { get; init; }
    public EntityInfo EntityInfo { get; init; } = default!;
}

public record SavingsAccount : AbstractAccount
{
    public InterestRate InterestRate { get; init; } = default!;
}

public record CheckingAccount : AbstractAccount
{
    public Overdraft Overdraft { get; init; } = default!;
}

public record EntityInfo
{
    public string Key { get; init; } = default!;
}

public record InterestRate
{
    public decimal Rate { get; init; }
}

public record Overdraft
{
    public decimal Limit { get; init; }
}

public class InheritedDbContext(DbContextOptions opts) : DbContext(opts)
{
    public DbSet<AbstractAccount> Accounts => Set<AbstractAccount>();
    public DbSet<SavingsAccount> SavingsAccounts => Set<SavingsAccount>();
    public DbSet<CheckingAccount> CheckingAccounts => Set<CheckingAccount>();
    public DbSet<Client> Clients => Set<Client>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<AbstractAccount>(r => r.OwnsOne(r => r.EntityInfo));
        modelBuilder.Entity<SavingsAccount>(r => r.OwnsOne(r => r.InterestRate));
        modelBuilder.Entity<CheckingAccount>(r => r.OwnsOne(r => r.Overdraft));
        modelBuilder.Entity<Client>(r => r.OwnsOne(r => r.EntityInfo));
    }
}
