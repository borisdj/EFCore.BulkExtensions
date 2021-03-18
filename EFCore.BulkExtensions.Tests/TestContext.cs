using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.Extensions.Configuration;
using NetTopologySuite.Geometries;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;

namespace EFCore.BulkExtensions.Tests
{
    public class TestContext : DbContext
    {
        public DbSet<Item> Items { get; set; }
        public DbSet<ItemHistory> ItemHistories { get; set; }

        public DbSet<UserRole> UserRoles { get; set; }

        public DbSet<Document> Documents { get; set; }
        public DbSet<Person> Persons { get; set; }
        public DbSet<Student> Students { get; set; }
        public DbSet<Teacher> Teachers { get; set; }
        public DbSet<Info> Infos { get; set; }
        public DbSet<ChangeLog> ChangeLogs { get; set; }
        public DbSet<ItemLink> ItemLinks { get; set; }
        public DbSet<Address> Addresses { get; set; }

        public DbSet<Parent> Parents { get; set; }
        public DbSet<ParentDetail> ParentDetails { get; set; }
        public DbSet<Child> Children { get; set; }

        public DbSet<Setting> Settings { get; set; }

        public TestContext(DbContextOptions options) : base(options)
        {
            Database.EnsureCreated();

            if (Database.IsSqlServer())
            {
                Database.ExecuteSqlRaw(@"if exists(select 1 from systypes where name='UdttIntInt') drop type UdttIntInt");
                Database.ExecuteSqlRaw(@"create type UdttIntInt AS TABLE(C1 int not null, C2 int not null)");
            }
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.RemovePluralizingTableNameConvention();

            modelBuilder.Entity<UserRole>().HasKey(a => new { a.UserId, a.RoleId });

            modelBuilder.Entity<Info>(e => { e.Property(p => p.ConvertedTime).HasConversion((value) => value.AddDays(1), (value) => value.AddDays(-1)); });
            modelBuilder.Entity<Info>().Property(e => e.InfoType).HasConversion(new EnumToStringConverter<InfoType>());
            modelBuilder.Entity<ChangeLog>().OwnsOne(e => e.Audit, b => b.Property(e => e.InfoType).HasConversion(new EnumToStringConverter<InfoType>()));

            modelBuilder.Entity<Person>().HasIndex(a => a.Name).IsUnique(); // In SQLite UpdateByColumn(nonPK) requires it has UniqueIndex

            if (Database.IsSqlServer())
            {
                modelBuilder.Entity<Document>().Property(p => p.ContentLength).HasComputedColumnSql($"(CONVERT([int], len([{nameof(Document.Content)}])))");

                modelBuilder.Entity<UdttIntInt>(entity => { entity.HasNoKey(); });
            }
            else if (Database.IsSqlite())
            {
                modelBuilder.Entity<Document>().Property(p => p.VersionChange).ValueGeneratedOnAddOrUpdate().IsConcurrencyToken().HasDefaultValueSql("CURRENT_TIMESTAMP");

                modelBuilder.Entity<Address>().Ignore(p => p.Location);
            }

            modelBuilder.Entity<Setting>().Property(e => e.Settings).HasConversion<string>();

            // [Timestamp] alternative:
            //modelBuilder.Entity<Document>().Property(x => x.RowVersion).HasColumnType("timestamp").ValueGeneratedOnAddOrUpdate().HasConversion(new NumberToBytesConverter<ulong>()).IsConcurrencyToken();

            //modelBuilder.Entity<Item>().HasQueryFilter(p => p.Description != "1234"); // For testing Global Filter
        }
    }

    public static class ContextUtil
    {
        public static DbServer DbServer { get; set; }

        public static DbContextOptions GetOptions(IInterceptor dbInterceptor) => GetOptions(new[] { dbInterceptor });
        public static DbContextOptions GetOptions(IEnumerable<IInterceptor> dbInterceptors = null) => GetOptions<TestContext>(dbInterceptors);

        public static DbContextOptions GetOptions<TDbContext>(IEnumerable<IInterceptor> dbInterceptors = null, string databaseName = nameof(EFCoreBulkTest))
            where TDbContext: DbContext
        {
            var optionsBuilder = new DbContextOptionsBuilder<TDbContext>();

            if (DbServer == DbServer.SqlServer)
            {
                var connectionString = GetSqlServerConnectionString(databaseName);

                // ALTERNATIVELY (Using MSSQLLocalDB):
                //var connectionString = $@"Data Source=(localdb)\MSSQLLocalDB;Database={databaseName};Trusted_Connection=True;MultipleActiveResultSets=True";

                //optionsBuilder.UseSqlServer(connectionString); // Can NOT Test with UseInMemoryDb (Exception: Relational-specific methods can only be used when the context is using a relational)
                optionsBuilder.UseSqlServer(connectionString, opt => opt.UseNetTopologySuite()); // NetTopologySuite for Geometry / Geometry types
            }
            else if (DbServer == DbServer.Sqlite)
            {
                string connectionString = GetSqliteConnectionString(databaseName);
                optionsBuilder.UseSqlite(connectionString);

                // ALTERNATIVELY:
                //string connectionString = (new SqliteConnectionStringBuilder { DataSource = $"{databaseName}Lite.db" }).ToString();
                //optionsBuilder.UseSqlite(new SqliteConnection(connectionString));
            }
            else
            {
                throw new NotSupportedException($"Database {DbServer} is not supported. Only SQL Server and SQLite are Currently supported.");
            }

            if (dbInterceptors?.Any() == true)
            {
                optionsBuilder.AddInterceptors(dbInterceptors);
            }

            return optionsBuilder.Options;
        }

        private static IConfiguration GetConfiguration()
        {
            var configBuilder = new ConfigurationBuilder()
                .AddJsonFile("testsettings.json", optional: false)
                .AddJsonFile("testsettings.local.json", optional: true);

            return configBuilder.Build();
        }

        public static string GetSqlServerConnectionString(string databaseName)
        {
            return GetConfiguration().GetConnectionString("SqlServer").Replace("{databaseName}", databaseName);
        }

        public static string GetSqliteConnectionString(string databaseName)
        {
            return GetConfiguration().GetConnectionString("Sqlite").Replace("{databaseName}", databaseName);
        }
    }

    public static class ModelBuilderExtensions
    {
        public static void RemovePluralizingTableNameConvention(this ModelBuilder modelBuilder)
        {
            foreach (IMutableEntityType entity in modelBuilder.Model.GetEntityTypes())
            {
                if (!entity.IsOwned() && entity.BaseType == null) // without this exclusion OwnedType would not be by default in Owner Table
                {
                    entity.SetTableName(entity.ClrType.Name);
                }
            }
        }
    }

    public class Item
    {
        public int ItemId { get; set; }

        public string Name { get; set; }

        public string Description { get; set; }

        public int Quantity { get; set; }

        public decimal? Price { get; set; }

        //[Column(TypeName = (nameof(DateTime)))] // Column to be of DbType 'datetime' instead of default 'datetime2'
        public DateTime TimeUpdated { get; set; }

        public ICollection<ItemHistory> ItemHistories { get; set; }
    }

    // ItemHistory is used to test bulk Ops to multiple tables(Item and ItemHistory), to test Guid as PK and to test other Schema(his)
    [Table(nameof(ItemHistory), Schema = "his")]
    public class ItemHistory
    {
        public Guid ItemHistoryId { get; set; }

        public int ItemId { get; set; }
        public virtual Item Item { get; set; }

        public string Remark { get; set; }
    }

    // UserRole is used to test tables with Composite PrimaryKey
    public class UserRole
    {
        [Key]
        public int UserId { get; set; }

        [Key]
        public int RoleId { get; set; }

        public string Description { get; set; }
    }

    // User Defined Table Type
    public class UdttIntInt
    {
        public int C1 { get; set; }
        public int C2 { get; set; }
    }

    // Person, Instructor nad Student are used to test Bulk with Shadow Property and Discriminator column
    public abstract class Person
    {
        public int PersonId { get; set; }

        public string Name { get; set; }
    }

    public class Student : Person
    {
        public string Subject { get; set; }
    }

    public class Address
    {
        public int AddressId { get; set; }
        public string Street { get; set; }

        public Geometry Location { get; set; }
    }

    public class Teacher : Person
    {
        public string Class { get; set; }
    }

    // For testing Computed Columns
    public class Document
    {
        public int DocumentId { get; set; }

        [Required]
        public string Content { get; set; }

        [Timestamp]
        public byte[] VersionChange { get; set; }

        //public ulong RowVersion { get; set; }

        [DatabaseGenerated(DatabaseGeneratedOption.Computed)] // Computed columns have to be configured with Fluent API
        public int ContentLength { get; set; }
    }

    public enum InfoType
    {
        InfoTypeA,
        InfoTypeB
    }

    // For testring ValueConversion
    public class Info
    {
        public long InfoId { get; set; }

        public string Message { get; set; }

        public string Note { get; protected set; } // To test protected Setter

        public DateTime ConvertedTime { get; set; }

        public InfoType InfoType { get; set; }
    }

    public enum SettingsEnum
    {
        Sett1,
        Sett2
    }
    // For testing Convertible Property (Key Settings in modelBuilder configured as 'nvarchar' insted of 'int')
    public class Setting
    {
        [Key]
        public SettingsEnum Settings { get; set; }
        [Required]
        [MaxLength(20)]
        public string Value { get; set; }
    }


    // For testing ForeignKey Shadow Properties
    public class ItemLink
    {
        public int ItemLinkId { get; set; }
        public virtual Item Item { get; set; }
    }

    // For testing OwnedTypes
    public class ChangeLog
    {
        public int ChangeLogId { get; set; }

        public string Description { get; set; }

        public Audit Audit { get; set; }

        //public AuditExtended AuditExtended { get; set; }

        //public AuditExtended AuditExtendedSecond { get; set; }
    }

    [Owned]
    public class Audit
    {
        [Column(nameof(ChangedBy))] // for setting custom column name, in this case prefix OwnedType_ ('Audit_') removed, so column would be only ('ChangedBy')
        public string ChangedBy { get; set; } // default Column name for Property of OwnedType is OwnedType_Property ('Audit_ChangedBy')

        public bool IsDeleted { get; set; }

        [NotMapped] // alternatively in OnModelCreating(): modelBuilder.Entity<Audit>().Ignore(a => a.ChangedTime);
        public DateTime? ChangedTime { get; set; }

        public InfoType InfoType { get; set; }
    }

    [Owned]
    public class AuditExtended
    {
        public string CreatedBy { get; set; }

        [NotMapped]
        public DateTime? CreatedTime { get; set; }

        [NotMapped]
        public string Remark { get; set; }
    }

    // For testing BatchUpdate expressions that use nested queries
    public class Parent
    {
        public ICollection<Child> Children { get; set; }
        public decimal CombinedChildBalance { get; set; }
        public string Description { get; set; }
        public virtual ParentDetail Details { get; set; }
        public int ParentId { get; set; }
        public decimal Value { get; set; }
    }

    public class ParentDetail
    {
        public int ParentDetailId { get; set; }

        public virtual Parent Parent { get; set; }
        public int ParentId { get; set; }

        public string Notes { get; set; }
    }

    public class Child
    {
        public int ChildId { get; set; }
        public bool IsEnabled { get; set; }
        public decimal Value { get; set; }

        public virtual Parent Parent { get; set; }
        public int ParentId { get; set; }
    }
}
