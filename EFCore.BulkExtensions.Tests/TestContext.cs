using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

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

        public TestContext(DbContextOptions options) : base(options)
        {
            Database.EnsureCreated();
        }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.RemovePluralizingTableNameConvention();

            modelBuilder.Entity<UserRole>().HasKey(a => new { a.UserId, a.RoleId });

            modelBuilder.Entity<Info>(e => { e.Property(p => p.ConvertedTime).HasConversion((value) => value.AddDays(1), (value) => value.AddDays(-1)); });
            modelBuilder.Entity<Info>().Property(e => e.InfoType).HasConversion(new EnumToStringConverter<InfoType>());

            modelBuilder.Entity<Person>().HasIndex(a => a.Name).IsUnique(); // In SQLite UpdateByColumn(nonPK) requires it has UniqueIndex

            if (Database.IsSqlServer())
            {
                modelBuilder.Entity<Document>().Property(p => p.ContentLength).HasComputedColumnSql($"(CONVERT([int], len([{nameof(Document.Content)}])))");
            }
            else if (Database.IsSqlite())
            {
                modelBuilder.Entity<Document>().Property(p => p.VersionChange).ValueGeneratedOnAddOrUpdate().IsConcurrencyToken().HasDefaultValueSql("CURRENT_TIMESTAMP");
            }

            // [Timestamp] alternative:
            //modelBuilder.Entity<Document>().Property(x => x.RowVersion).HasColumnType("timestamp").ValueGeneratedOnAddOrUpdate().HasConversion(new NumberToBytesConverter<ulong>()).IsConcurrencyToken();

            //modelBuilder.Entity<Item>().HasQueryFilter(p => p.Description != "1234"); // For testing Global Filter
        }
    }

    public static class ContextUtil
    {
        public static DbServer DbServer { get; set; }

        public static DbContextOptions GetOptions()
        {
            var databaseName = nameof(EFCoreBulkTest);
            var optionsBuilder = new DbContextOptionsBuilder<TestContext>();

            if (DbServer == DbServer.SqlServer)
            {
                var connectionString = $"Server=localhost;Database={databaseName};Trusted_Connection=True;MultipleActiveResultSets=true";
                optionsBuilder.UseSqlServer(connectionString); // Can NOT Test with UseInMemoryDb (Exception: Relational-specific methods can only be used when the context is using a relational)
            }
            else if (DbServer == DbServer.Sqlite)
            {
                string connectionString = $"Data Source={databaseName}.db";
                optionsBuilder.UseSqlite(connectionString);

                // ALTERNATIVELY:
                //string connectionString = (new SqliteConnectionStringBuilder { DataSource = $"{databaseName}Lite.db" }).ToString();
                //optionsBuilder.UseSqlite(new SqliteConnection(connectionString));
            }
            else
            {
                throw new NotSupportedException($"Database {DbServer} is not supported. Only SQL Server and SQLite are Currently supported.");
            }

            return optionsBuilder.Options;
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
        public int InfoId { get; set; }

        public string Message { get; set; }

        public DateTime ConvertedTime { get; set; }

        public InfoType InfoType { get; set; }
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
}
