using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System;
using System.Diagnostics.CodeAnalysis;
using EFCore.BulkExtensions.SqlAdapters;

namespace EFCore.BulkExtensions.Tests.ValueConverters;

public class VcDbContext : DbContext
{
    private readonly SqlType _sqlType;

    [SuppressMessage("ReSharper", "VirtualMemberCallInConstructor")]
    public VcDbContext(string databaseName, SqlType sqlType) 
        : base(new ContextUtil(sqlType).GetOptions<VcDbContext>(databaseName: databaseName))
    {
        _sqlType = sqlType;
        
        Database.EnsureDeleted();
        Database.EnsureCreated();
    }

    public DbSet<VcModel> VcModels { get; set; } = null!;

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        modelBuilder.Entity<VcModel>(cfg =>
        {
            cfg.HasKey(y => y.Id);
            cfg.Property(y => y.Id).UseIdentityColumn();
            
            cfg.Property(y => y.Enum)
                .HasColumnType(_sqlType == SqlType.PostgreSql ? "text" : "nvarchar(4000)")
                .HasConversion<string>();

            cfg.Property(y => y.LocalDate).HasColumnType("date").HasConversion(new LocalDateValueConverter());
        });
    }

    public class LocalDateValueConverter : ValueConverter<LocalDate, DateTime>
    {
        public LocalDateValueConverter() 
            : base((LocalDate i) => ToProvider(i), (DateTime d) => FromProvider(d), null)
        {
        }

        private static DateTime ToProvider(LocalDate localDate)
        {
            return new DateTime(localDate.Year, localDate.Month, localDate.Day, 0, 0, 0, DateTimeKind.Unspecified);
        }

        private static LocalDate FromProvider(DateTime dateTime)
        {
            return new LocalDate(dateTime.Year, dateTime.Month, dateTime.Day);
        }
    }
}
