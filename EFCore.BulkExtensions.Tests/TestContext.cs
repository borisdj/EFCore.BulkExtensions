using EFCore.BulkExtensions.SqlAdapters;
using HotChocolate;
using HotChocolate.Types;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Conventions;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Microsoft.Extensions.Configuration;
using NetTopologySuite.Geometries;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Text.Json;
using Npgsql;

namespace EFCore.BulkExtensions.Tests;

public class TestContext : DbContext
{

    public DbSet<Item> Items { get; set; } = null!;
    public DbSet<ItemHistory> ItemHistories { get; set; } = null!;

    public DbSet<UserRole> UserRoles { get; set; } = null!;

    public DbSet<Document> Documents { get; set; } = null!;
    public DbSet<Letter> Letters { get; set; } = null!;

    public DbSet<Storage> Storages { get; set; } = null!;

    public DbSet<File> Files { get; set; } = null!;

    public DbSet<FilePG> FilePGs { get; set; } = null!;

    public DbSet<Box> Boxes { get; set; } = null!;
    public DbSet<Person> Persons { get; set; } = null!;
    public DbSet<Student> Students { get; set; } = null!;
    public DbSet<Teacher> Teachers { get; set; } = null!;
    public DbSet<Entry> Entries { get; set; } = null!;
    public DbSet<EntryArchive> EntryArchives { get; set; } = null!;
    public DbSet<EntryPrep> EntryPreps { get; set; } = null!;
    public DbSet<Modul> Moduls { get; set; } = null!;
    public DbSet<Info> Infos { get; set; } = null!;
    public DbSet<ChangeLog> ChangeLogs { get; set; } = null!;
    public DbSet<Tracker> Trackers { get; set; } = null!;
    public DbSet<AuditData> AuditData { get; set; } = null!;
    public DbSet<ItemLink> ItemLinks { get; set; } = null!;
    public DbSet<Address> Addresses { get; set; } = null!;
    public DbSet<Category> Categories { get; set; } = null!;

    public DbSet<Parent> Parents { get; set; } = null!;
    public DbSet<ParentDetail> ParentDetails { get; set; } = null!;
    public DbSet<Child> Children { get; set; } = null!;

    public DbSet<Animal> Animals { get; set; } = null!;

    public DbSet<Setting> Settings { get; set; } = null!;

    public DbSet<LogPersonReport> LogPersonReports { get; set; } = null!;

    public DbSet<AtypicalRowVersionEntity> AtypicalRowVersionEntities { get; set; } = null!;

    public DbSet<AtypicalRowVersionConverterEntity> AtypicalRowVersionConverterEntities { get; set; } = null!;

    public DbSet<Event> Events { get; set; } = null!;

    public DbSet<Archive> Archives { get; set; } = null!;

    public DbSet<Counter> Counters { get; set; } = null!;

    public DbSet<Source> Sources { get; set; } = null!;

    public DbSet<Department> Departments { get; set; } = null!;
    public DbSet<Division> Divisions { get; set; } = null!;
    public DbSet<PrivateKey> PrivateKeys { get; set; } = null!;
    public DbSet<Wall> Walls { get; set; } = null!;

    public DbSet<TimeRecord> TimeRecords { get; set; } = null!;

    public DbSet<Customer> Customers { get; set; } = null!;

    public DbSet<Template> Templates { get; set; } = null!;

    public DbSet<Author> Authors { get; set; } = null!;

    public DbSet<Partner> Partners { get; set; } = null!;

    public DbSet<GraphQLModel> GraphQLModels { get; set; } = null!;

    public virtual DbSet<Mod> Mods { get; set; } = default!;

    public DbSet<TrayType> TrayTypes { get; set; } = null!;

    public virtual DbSet<ChildType> ChildTypes { get; set; } = null!;
    public virtual DbSet<ParentType> ParentTypes { get; set; } = null!;

    public static bool UseTopologyPostgres { get; set; } = true; // needed for object Address with Geo. props
    // 'No suitable constructor was found for entity type 'LineString'. The following constructors had parameters that could not be bound to properties of the entity type: 
    // Cannot bind 'points' in 'LineString(Coordinate[] points)'  Cannot bind 'points', 'factory' in 'LineString(CoordinateSequence points, GeometryFactory factory)'

    public TestContext(DbContextOptions options) : base(options)
    {
        Database.EnsureCreated();
        // if for Postgres on test run get Npgsql Ex:[could not open .../postgis.control] then either install the plugin it or set UseTopologyPostgres to False;

        /*if (Database.IsSqlServer())
        {
            Database.ExecuteSqlRaw(@"if exists(select 1 from systypes where name='UdttIntInt') drop type UdttIntInt");
            Database.ExecuteSqlRaw(@"create type UdttIntInt AS TABLE(C1 int not null, C2 int not null)");
        }*/
    }

    protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
    {
        configurationBuilder.Conventions.Remove(typeof(TableNameFromDbSetConvention));
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        // modelBuilder.RemovePluralizingTableNameConvention(); // instead used ConfigureConventions
        
        modelBuilder.Entity<Mod>()
            .ToTable("Mods");

        modelBuilder.Entity<Mod>()
           .HasKey(x => new { x.Id, x.PlayerId });

        modelBuilder.Entity<Mod>()
           .Property(x => x.Id)
           .HasConversion(x => x.Value, v => ModId.Create(v))
           .ValueGeneratedNever()
           .IsRequired();

        modelBuilder.Entity<Mod>()
           .Property(x => x.PlayerId)
           .HasConversion(x => x.Value, v => PlayerId.Create(v))
           .ValueGeneratedNever()
           .IsRequired();

        modelBuilder.Entity<Mod>()
           .HasMany(x => x.Stats)
           .WithOne();

        modelBuilder.Entity<Mod>()
           .Metadata
           .FindNavigation(nameof(Mod.Stats))!
               .SetPropertyAccessMode(PropertyAccessMode.Field);

        modelBuilder.Entity<ModStat>()
            .ToTable("ModStat");

        modelBuilder.Entity<ModStat>()
            .HasKey(x => new { x.ModId, x.PlayerId });

        modelBuilder.Entity<ModStat>()
            .Property(x => x.ModId)
            .HasConversion(x => x.Value, v => ModId.Create(v))
            .ValueGeneratedNever()
            .IsRequired();

        modelBuilder.Entity<ModStat>()
            .Property(x => x.PlayerId)
            .HasConversion(x => x.Value, v => PlayerId.Create(v))
            .ValueGeneratedNever()
            .IsRequired();

        modelBuilder.Entity<Info>(e => { e.Property(p => p.ConvertedTime).HasConversion((value) => value.AddDays(1), (value) => value.AddDays(-1)); });

        modelBuilder.Entity<UserRole>().HasKey(a => new { a.UserId, a.RoleId });

        modelBuilder.Entity<Info>(e => { e.Property(p => p.ConvertedTime).HasConversion((value) => value.AddDays(1), (value) => value.AddDays(-1)); });
        modelBuilder.Entity<Info>().Property(p => p.InfoType).HasConversion(new EnumToStringConverter<InfoType>());
        modelBuilder.Entity<Info>().Property(p => p.DateTimeOff).HasConversion(new DateTimeOffsetToBinaryConverter());

        modelBuilder.Entity<Info>(e => { e.Property("LogData"); });
        modelBuilder.Entity<Info>(e => { e.Property("TimeCreated"); });
        modelBuilder.Entity<Info>(e => { e.Property("Remark"); });

        modelBuilder.Entity<Wall>().HasKey(x => x.Id);
        modelBuilder.Entity<Wall>().Property(x => x.Id).ValueGeneratedNever();
        modelBuilder.Entity<Wall>().Property(x => x.WallTypeValue).HasConversion(new EnumToStringConverter<WallType>());
        modelBuilder.Entity<Wall>().Property(x => x.WallCategory).HasConversion(new EnumToStringConverter<WallCategory>());

        modelBuilder.Entity<Customer>().HasIndex(p => p.Name).IsUnique();

        modelBuilder.Entity<TimeRecord>().OwnsOne(a => a.Source,
            b => b.Property(p => p.Type).HasConversion(new EnumToNumberConverter<TimeRecordSourceType, int>()));

        modelBuilder.Entity<ChangeLog>().OwnsOne(a => a.Audit,
            b => b.Property(p => p.InfoType).HasConversion(new EnumToStringConverter<InfoType>()));

        modelBuilder.Entity<Person>().HasIndex(a => a.Name)
            .IsUnique(); // In SQLite UpdateByColumn(nonPK) requires it has UniqueIndex

        modelBuilder.Entity<Document>().Property(p => p.IsActive).HasDefaultValue(true);
        modelBuilder.Entity<Document>().Property(p => p.Tag).HasDefaultValue("DefaultData");
        if (Database.IsSqlServer())
        {
            modelBuilder.HasSequence<long>("OrderNumber").StartsAt(80000000);
            modelBuilder.Entity<Document>().Property(o => o.OrderNumber).HasDefaultValueSql<long>("NEXT VALUE FOR OrderNumber");
        }

        modelBuilder.Entity<Log>().ToTable(nameof(Log));
        modelBuilder.Entity<LogPersonReport>().ToTable(nameof(LogPersonReport));

        modelBuilder.Entity<FilePG>().Ignore(p => p.Formats);

        modelBuilder.Entity<ItemLink>().Property<string>("Data");
        
        if (Database.IsSqlServer() || Database.IsNpgsql())
        {
            modelBuilder.Entity<Author>().OwnsOne(
                author => author.Contact, ownedNavigationBuilder =>
                {
                    ownedNavigationBuilder.ToJson();
                    ownedNavigationBuilder.OwnsOne(contactDetails => contactDetails.Address);
                });
        }
        
        if (Database.IsSqlServer())
        {
            modelBuilder.Entity<GraphQLModel>().Property(x => x.Id).HasDefaultValueSql("NEWID()");

            modelBuilder.Entity<Document>().Property(p => p.DocumentId).HasDefaultValueSql("NEWID()");
            modelBuilder.Entity<Document>().Property(p => p.ContentLength).HasComputedColumnSql($"(CONVERT([int], len([{nameof(Document.Content)}])))");

            modelBuilder.Entity<Storage>().ToTable(nameof(Storage), b => b.IsTemporal());

            modelBuilder.Entity<UdttIntInt>(entity => { entity.HasNoKey(); });

            modelBuilder.Entity<Category>().Property(p => p.HierarchyDescription).HasColumnType("hierarchyid");

            modelBuilder.Entity<Division>().Property(p => p.Id).HasDefaultValueSql("NEWSEQUENTIALID()");
            modelBuilder.Entity<Department>().Property(p => p.Id).HasDefaultValueSql("NEWSEQUENTIALID()");

            //modelBuilder.Entity<Address>().Property(p => p.LocationGeometry).HasColumnType("geometry");

            //modelBuilder.Entity<SequentialInfo>().HasKey(a => a.Id);
            //SqlServerPropertyBuilderExtensions.UseHiLo(modelBuilder.Entity<SequentialInfo>().Property(a => a.Id), name: "SequenceData", schema: "dbo");
            //modelBuilder.HasSequence<int>("SequenceData", "dbo").StartsAt(10).IncrementsBy(5);

            modelBuilder.Entity<Tracker>().OwnsOne(t => t.Location);
        }
        else
        {
            modelBuilder.Entity<Tracker>().OwnsOne(t => t.Location).Ignore(p => p.Location); // Point only on SqlServer
        }

        if (Database.IsSqlite() || Database.IsNpgsql() || Database.IsMySql())
        {
            modelBuilder.Entity<Category>().Ignore(p => p.HierarchyDescription);

            modelBuilder.Entity<Event>().Ignore(p => p.TimeCreated);
        }

        if (Database.IsSqlite())
        {
            modelBuilder.Entity<File>().Property(p => p.VersionChange).ValueGeneratedOnAddOrUpdate().IsConcurrencyToken().HasDefaultValueSql("CURRENT_TIMESTAMP");

            modelBuilder.Entity<ItemHistory>().ToTable(nameof(ItemHistory));

            modelBuilder.Entity<Address>()
                .Property(p => p.GeoLine)
                .HasSrid(4326);

            modelBuilder.Entity<Address>()
                .Property(p => p.GeoPoint)
                .HasSrid(4326);
        }

        if (Database.IsMySql())
        {
            modelBuilder.Entity<Address>().Ignore(p => p.LocationGeography);
            modelBuilder.Entity<Address>().Ignore(p => p.LocationGeometry);
            modelBuilder.Entity<Address>().Ignore(p => p.GeoLine);
        }

        if (Database.IsNpgsql())
        {
            modelBuilder.Entity<GraphQLModel>().Property(x => x.Id).HasDefaultValueSql("gen_random_uuid()");

            if (UseTopologyPostgres)
            {
                modelBuilder.Entity<Address>().Property(p => p.LocationGeometry).HasColumnType("geometry (point, 2180)").HasSrid(2180);
            }
            else
            {
                modelBuilder.Entity<Address>().Ignore(p => p.LocationGeography);
                modelBuilder.Entity<Address>().Ignore(p => p.LocationGeometry);
            }

            modelBuilder.Entity<FilePG>().Property(p => p.Formats).HasColumnType("text[]");

            modelBuilder.Entity<Event>().Property(p => p.TimeCreated).HasColumnType("timestamp"); // with annotation defined as "datetime2(3)" so here corrected for PG ("timestamp" in short for "timestamp without time zone")
            modelBuilder.Entity<Event>().Property(p => p.TimeUpdated).HasColumnType("timestamp(2)");

            modelBuilder.Entity<Box>().Property(p => p.ElementContent).HasColumnType("jsonb"); // with annotation not mapped since not used for others DBs
            modelBuilder.Entity<Box>().Property(p => p.DocumentContent).HasColumnType("jsonb"); // with annotation not mapped since not used for others DBs
        }
        else
        {
            modelBuilder.Entity<Partner>().Ignore(p => p.RowVersion);
        }

        //modelBuilder.Entity<Modul>(buildAction => { buildAction.HasNoKey(); });
        modelBuilder.Entity<Modul>().Property(et => et.Code).ValueGeneratedNever();

        modelBuilder.Entity<Setting>().Property(e => e.Settings).HasConversion<string>();

        modelBuilder.Entity<AtypicalRowVersionEntity>().HasKey(e => e.Id);
        modelBuilder.Entity<AtypicalRowVersionEntity>().Property(e => e.RowVersion).HasDefaultValue(0).IsConcurrencyToken().ValueGeneratedOnAddOrUpdate().Metadata.SetBeforeSaveBehavior(PropertySaveBehavior.Save);
        modelBuilder.Entity<AtypicalRowVersionEntity>().Property(e => e.SyncDevice).IsRequired(true).IsConcurrencyToken().HasDefaultValue("");

        if (!Database.IsNpgsql() && !Database.IsMySql())
        {
            modelBuilder.Entity<AtypicalRowVersionConverterEntity>().Property(e => e.RowVersionConverted).HasConversion(new NumberToBytesConverter<long>()).HasColumnType("timestamp").IsRowVersion().IsConcurrencyToken();
        }

        modelBuilder.Entity<Parent>().Property(parent => parent.PhoneNumber)
            .HasColumnType("varchar(12)").HasMaxLength(12).HasField("_phoneNumber").IsRequired();

        modelBuilder.Entity<PrivateKey>(c =>
        {
            c.HasKey("Id");
        });

        modelBuilder.Entity<ChildType>(entity =>
        {
            entity.ToTable(nameof(ChildType));
            entity.HasKey(e => e.ChildTypeKey).HasName("PK_ChildType");
            entity.Property(e => e.ChildTypeKey).IsRequired().ValueGeneratedNever();
            entity.Property(e => e.ParentTypeKey).IsRequired();
            entity.Property(e => e.ChildLabel).IsRequired().HasMaxLength(100).IsUnicode(false);
            entity.HasOne(d => d.ParentType)
                .WithMany(p => p.Children)
                .HasForeignKey(d => d.ParentTypeKey)
                .OnDelete(DeleteBehavior.Cascade)
                .HasConstraintName("FK_ChildType_ParentType");
        });
        modelBuilder.Entity<ParentType>(entity =>
        {
            entity.ToTable(nameof(ParentType));
            entity.HasKey(e => e.ParentTypeKey).HasName("PK_ParentType");
            entity.Property(e => e.ParentTypeKey).IsRequired().ValueGeneratedNever();
            entity.Property(e => e.ParentLabel).IsRequired().HasMaxLength(100).IsUnicode(false);
        });

        //modelBuilder.Entity<Person>().HasDiscriminator<string>("Discriminator2").HasValue<Student>("Student").HasValue<Teacher>("Teacher"); // name of classes are default values

        // [Timestamp] alternative:
        //modelBuilder.Entity<Document>().Property(x => x.RowVersion).HasColumnType("timestamp").ValueGeneratedOnAddOrUpdate().HasConversion(new NumberToBytesConverter<ulong>()).IsConcurrencyToken();

        //modelBuilder.Entity<Item>().HasQueryFilter(p => p.Description != "1234"); // For testing Global Filter // TODO add new test
    }
}

public class OracleTestContext : DbContext
{

    public DbSet<Item> Items { get; set; } = null!;
    public DbSet<ItemHistory> ItemHistories { get; set; } = null!;
    public DbSet<ItemCategory> Categories { get; set; } = null!;

    public OracleTestContext(DbContextOptions options) : base(options)
    {
        Database.EnsureCreated();
    }

    protected override void ConfigureConventions(ModelConfigurationBuilder configurationBuilder)
    {
        configurationBuilder.Conventions.Remove(typeof(TableNameFromDbSetConvention));
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        ApplyTableName(modelBuilder);
    }
    private static string ToScreamingSnakeCase(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        return string.Concat(input.Select((c, i) =>
            i > 0 && char.IsUpper(c) && !char.IsUpper(input[i - 1])
                ? "_" + c
                : c.ToString()))
            .ToUpper();
    }
    public static void ApplyTableName(ModelBuilder modelBuilder)
    {
        foreach (var entity in modelBuilder.Model.GetEntityTypes())
        {
            // Přizpůsobení názvu tabulky
            var tableName = entity.GetTableName();
            if (tableName != null)
            {
                var newTableName = ToScreamingSnakeCase(tableName);
                entity.SetTableName(newTableName);

                // Přizpůsobení názvů sloupců
                foreach (var property in entity.GetProperties())
                {
                    var columnName = property.GetColumnName(StoreObjectIdentifier.Table(newTableName, null));
                    if (columnName != null)
                    {
                        var newColumnName = ToScreamingSnakeCase(columnName);
                        property.SetColumnName(newColumnName);
                    }
                }
            }
        }
    }
}

public static class ContextUtil
{
    static IDbServer? _dbServerMapping;
    static SqlType _databaseType;

    // TODO: Pass DbService through all the GetOptions methods as a parameter and eliminate this property so the automated tests
    // are thread safe
    public static SqlType DatabaseType
    {
        get => _databaseType;
        set
        {
            _databaseType = value;
            _dbServerMapping = value switch
            {
                SqlType.SqlServer => new SqlAdapters.SqlServer.SqlServerDbServer(),
                SqlType.Sqlite => new SqlAdapters.Sqlite.SqliteDbServer(),
                SqlType.PostgreSql => new SqlAdapters.PostgreSql.PostgreSqlDbServer(),
                SqlType.MySql => new SqlAdapters.MySql.MySqlDbServer(),
                SqlType.Oracle => new SqlAdapters.Oracle.OracleDbServer(),
                _ => throw new NotImplementedException(),
            };
        }
    }

    public static DbContextOptions GetOptions(IInterceptor dbInterceptor) => GetOptions(new[] { dbInterceptor });
    public static DbContextOptions GetOptions(IEnumerable<IInterceptor>? dbInterceptors = null) => GetOptions<TestContext>(dbInterceptors);

    public static DbContextOptions GetOptions<TDbContext>(IEnumerable<IInterceptor>? dbInterceptors = null, string databaseName = nameof(EFCoreBulkTest))
        where TDbContext : DbContext
        => GetOptions<TDbContext>(ContextUtil.DatabaseType, dbInterceptors, databaseName);

    public static DbContextOptions GetOptions<TDbContext>(SqlType dbServerType, IEnumerable<IInterceptor>? dbInterceptors = null, string databaseName = nameof(EFCoreBulkTest))
        where TDbContext : DbContext
    {
        var optionsBuilder = new DbContextOptionsBuilder<TDbContext>();

        if (dbServerType == SqlType.SqlServer)
        {
            var connectionString = GetSqlServerConnectionString(databaseName);

            // ALTERNATIVELY (Using MSSQLLocalDB):
            //var connectionString = $@"Data Source=(localdb)\MSSQLLocalDB;Database={databaseName};Trusted_Connection=True;MultipleActiveResultSets=True";

            //optionsBuilder.UseSqlServer(connectionString); // Can NOT Test with UseInMemoryDb (Exception: Relational-specific methods can only be used when the context is using a relational)
            //optionsBuilder.UseSqlServer(connectionString, opt => opt.UseNetTopologySuite()); // NetTopologySuite for Geometry / Geometry types
            optionsBuilder.UseSqlServer(connectionString, opt =>
            {
                opt.UseNetTopologySuite();
                opt.UseHierarchyId();
            });
        }
        else if (dbServerType == SqlType.Sqlite)
        {
            string connectionString = GetSqliteConnectionString(databaseName);
            optionsBuilder.UseSqlite(connectionString, opt =>
            {
                opt.UseNetTopologySuite();
            });
            SQLitePCL.Batteries.Init();

            // ALTERNATIVELY:
            //string connectionString = (new SqliteConnectionStringBuilder { DataSource = $"{databaseName}Lite.db" }).ToString();
            //optionsBuilder.UseSqlite(new SqliteConnection(connectionString));
        }
        else if (DatabaseType == SqlType.PostgreSql)
        {
            string connectionString = GetPostgreSqlConnectionString(databaseName);

            var builder = new NpgsqlDataSourceBuilder(connectionString)
                .EnableDynamicJson()
                .Build();

            if (TestContext.UseTopologyPostgres)
                optionsBuilder.UseNpgsql(builder, opt => opt.UseNetTopologySuite());
            else
                optionsBuilder.UseNpgsql(builder);
        }
        else if (DatabaseType == SqlType.MySql)
        {
            string connectionString = GetMySqlConnectionString(databaseName);
            optionsBuilder.UseMySql(connectionString, ServerVersion.AutoDetect(connectionString), opt => opt.UseNetTopologySuite());
        }
        else if (DatabaseType == SqlType.Oracle)
        {
            string connectionString = GetOracleConnectionString(databaseName);
            optionsBuilder.UseOracle(connectionString);
        }
        else
        {
            throw new NotSupportedException($"Database {dbServerType} is not supported. Only SQL Server and SQLite are Currently supported.");
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
#pragma warning disable CS8602 // Dereference of a possibly null reference.
        return GetConfiguration().GetConnectionString("SqlServer").Replace("{databaseName}", databaseName);
#pragma warning restore CS8602 // Dereference of a possibly null reference.
    }

    public static string GetSqliteConnectionString(string databaseName)
    {
#pragma warning disable CS8602 // Dereference of a possibly null reference.
        return GetConfiguration().GetConnectionString("Sqlite").Replace("{databaseName}", databaseName);
#pragma warning restore CS8602 // Dereference of a possibly null reference.
    }

    public static string GetPostgreSqlConnectionString(string databaseName)
    {
#pragma warning disable CS8602 // Dereference of a possibly null reference.
        return GetConfiguration().GetConnectionString("PostgreSql").Replace("{databaseName}", databaseName);
#pragma warning restore CS8602 // Dereference of a possibly null reference.
    }

    public static string GetMySqlConnectionString(string databaseName)
    {
#pragma warning disable CS8602 // Dereference of a possibly null reference.
        return GetConfiguration().GetConnectionString("MySql").Replace("{databaseName}", databaseName);
#pragma warning restore CS8602 // Dereference of a possibly null reference.
    }

    public static string GetOracleConnectionString(string databaseName)
    {
#pragma warning disable CS8602 // Dereference of a possibly null reference.
        return GetConfiguration().GetConnectionString("Oracle").Replace("{databaseName}", databaseName);
#pragma warning restore CS8602 // Dereference of a possibly null reference.
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

public class ChildType
{
    public long ChildTypeKey { get; set; }
    public long ParentTypeKey { get; set; }
    public string ChildLabel { get; set; } = default!;
    public virtual ParentType? ParentType { get; set; }
    public override string ToString()
    {
        return $"ChildType: ChildTypeKey={ChildTypeKey} ChildLabel={ChildLabel} ParentTypeKey={ParentTypeKey}";
    }
}

public class ParentType
{
    public long ParentTypeKey { get; set; }
    public string ParentLabel { get; set; } = default!;
    public virtual ICollection<ChildType> Children { get; set; } = [];
    public override string ToString()
    {
        return $"ParentType: ParentTypeKey={ParentTypeKey} ParentTypeLabel={ParentLabel}";
    }
}

public class Item
{
    public Item()
    {

    }
    public Item(int itemId, string name, string description, int quantity, decimal? price, DateTime? timeUpdated, ICollection<ItemHistory> itemHistories)
    {
        ItemId = itemId;
        Name = name;
        Description = description;
        Quantity = quantity;
        Price = price;
        TimeUpdated = timeUpdated;
        ItemHistories = itemHistories;
    }

    public int ItemId { get; set; }

    [MaxLength(50)]
    public string? Name { get; set; }

    public string? Description { get; set; }

    public int Quantity { get; set; }

    public decimal? Price { get; set; }

    //[Column(TypeName = (nameof(DateTime)))] // Column to be of DbType 'datetime' instead of default 'datetime2'
    public DateTime? TimeUpdated { get; set; }

    public ICollection<ItemHistory> ItemHistories { get; set; } = null!;

    public ItemCategory? Category { get; set; }
}

// ItemHistory is used to test bulk Ops to multiple tables(Item and ItemHistory), to test Guid as PK and to test other Schema(his)
[Table(nameof(ItemHistory)/*, Schema = "his"*/)] // different schema is not supported in Sqlite
public class ItemHistory
{
    public Guid ItemHistoryId { get; set; }

    public int ItemId { get; set; }
    public virtual Item Item { get; set; } = null!;

    public string Remark { get; set; } = null!;

    [Timestamp]
    public byte[]? RowVersion { get; set; }
}


public class ItemCategory
{
    [Key]
    public int Id { get; set; }

    [MaxLength(50)]
    public string? Name { get; set; }

}

// UserRole is used to test tables with Composite PrimaryKey
public class UserRole
{
    public UserRole()
    {

    }
    public UserRole(int userId, int roleId, string description)
    {
        UserId = userId;
        RoleId = roleId;
        Description = description;
    }

    [Key]
    public int UserId { get; set; }

    [Key]
    public int RoleId { get; set; }

    public string? Description { get; set; }
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

    public string Name { get; set; } = null!;

    //public string Discriminator { get; set; } = null!;
}

public class Student : Person
{
    public string Subject { get; set; } = null!;
}

public class Teacher : Person
{
    public string Class { get; set; } = null!;
}

public class Wall
{
    public long Id { get; set; }
    public WallType WallTypeValue { get; set; } = WallType.Clay;

    public WallCategory WallCategory { get; set; }
}

public enum WallType
{
    Brick,
    Clay
}

public enum WallCategory
{
    [Description("LowC")]
    Low,

    [Description("HighC")]
    High
}

public class TimeRecord
{
    public TimeRecord()
    {
        Source = new TimeRecordSource();
    }
    public int TimeRecordId { get; set; }

    public TimeRecordSource Source { get; set; }
}

[Owned]
public class TimeRecordSource
{
    public TimeRecordSourceType Type { get; set; }
    public string? Name { get; set; } = null;
    public string? Id { get; set; } = null;
}

public enum TimeRecordSourceType
{
    Unknown = 0,
    Device,
    Operator,
}

// To Test custom Destination and Source tables
public class Entry
{
    public int EntryId { get; set; }

    public string Name { get; set; } = null!;
}
public class EntryArchive
{
    [Key]
    public int EntryId { get; set; }

    public string Name { get; set; } = null!;
}
public class EntryPrep
{
    [Key]
    public int EntryPrepId { get; set; }

    public string NameInfo { get; set; } = null!;
}

public class Address
{
    public int AddressId { get; set; }
    public string Street { get; set; } = null!;
    public Geometry LocationGeography { get; set; } = null!;
    public Geometry LocationGeometry { get; set; } = null!;
    public LineString? GeoLine { get; set; }
    public Point? GeoPoint { get; set; }
}

public class Category
{
    public int CategoryId { get; set; }
    public string Name { get; set; } = null!;
    public HierarchyId HierarchyDescription { get; set; } = null!;
}

// For testing Computed columns Default values
public class Document
{
    //[DefaultValueSql("NEWID()")] // no native [DefaultValueSql] annotation so this is configured via FluentAPI in modelBuilder
    public Guid DocumentId { get; set; }

    //[DefaultValue(true)] // EF doesn't use DefaultValue attribute, not annotation, so this is configured via FluentAPI as well
    public bool? IsActive { get; set; }


    //HasDefaultValueSql
    [Required]
    public string Content { get; set; } = null!;

    //HasComputedColumnSql
    public string? Tag { get; set; }

    [DatabaseGenerated(DatabaseGeneratedOption.Computed)] // Computed columns also have to be configured with FluentAPI
    public int ContentLength { get; set; }

    public long OrderNumber { get; private set; }
}

// For testing parameterless constructor
public class Letter
{
    public Letter(string note)
    {
        Note = note;
    }
    public int LetterId { get; set; }

    public string Note { get; set; }
}

// For testing Temporal tables (configured via FluentAPI )
public class Storage
{
    public int StorageId { get; set; }

    public string Data { get; set; } = null!;

    [Timestamp]
    public byte[]? RowVersion { get; set; }
}

// For testing type 'jsonb' on Postgres
public class Box
{
    public int BoxId { get; set; }

    [NotMapped] // used only for Postgres so mapped wiht FluentAPI 
    public System.Text.Json.JsonElement ElementContent { get; set; }

    [NotMapped] // used only for Postgres so mapped wiht FluentAPI 
    public JsonDocument DocumentContent { get; set; } = null!;
}

// For testing TimeStamp Property and Column with Concurrency Lock
public class File
{
    [Column("Id")] // test different column Name, PK in this case
    public int FileId { get; set; }

    [Required]
    public string Description { get; set; } = null!;

    public byte[]? DataBytes { get; set; }

    [Timestamp]
    public byte[] VersionChange { get; set; } = Guid.NewGuid().ToByteArray();
    //public ulong RowVersion { get; set; }
}

// For testing TimeStamp Property and Column with Concurrency Lock
public class FilePG
{
    public int FilePGId { get; set; }

    public string? Description { get; set; }

    //[Column(TypeName = "text[]")] // set in Fluent
    public string[]? Formats { get; set; }

    [Timestamp]
    public uint Version { get; set; }
}

public enum InfoType
{
    InfoTypeA,
    InfoTypeB
}

public class Modul
{

    [Key]
    [Required]
    public string Code { get; set; } = null!;
    public string Name { get; set; } = null!;
}

// For testring ValueConversion
public class Info
{
    public Info()
    {
        logData = "logged";
        TimeCreated = DateTime.Now;
    }
    [Required]
    private string logData; // To test private Field with protected explicit getter/setter

    public long InfoId { get; set; }

    public string Message { get; set; } = null!;

    public DateTime ConvertedTime { get; set; }

    public InfoType InfoType { get; set; }

    public string Note { get; protected set; } = "NN"; // To test protected Setter

    public string? Remark { get; } // To test without Setter

    private DateTime TimeCreated { get; set; } // To test private Property

    [Required]
    private string LogData { get { return logData; } set { logData = value; } }

    public DateTimeOffset DateTimeOff { get; set; } // ValueConverter to Binary

    public string GetLogData() { return logData; }
    public DateTime GetDateCreated() { return TimeCreated.Date; }
}


public class Animal
{
    public int AnimalId { get; set; }

    [Required]
    public string Name { get; set; } = null!;
}

public class Mammal : Animal
{
    public string Category { get; set; } = null!;
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
    public string? Value { get; set; }
}


// For testing ForeignKey Shadow Properties
public class ItemLink
{
    public int ItemLinkId { get; set; }
    public virtual Item Item { get; set; } = null!;
}

// For testing OwnedTypes
public class ChangeLog
{
    public int ChangeLogId { get; set; }

    public string Description { get; set; } = null!;

    public Audit Audit { get; set; } = null!;

    public AuditExtended AuditExtended { get; set; } = null!;

    public AuditExtended AuditExtendedSecond { get; set; } = null!;
}

[Owned]
public class Audit
{

    [Column(nameof(ChangedBy))] // for setting custom column name, in this case prefix OwnedType_ ('Audit_') removed, so column would be only ('ChangedBy')
    public string ChangedBy { get; set; } = null!; // default Column name for Property of OwnedType is OwnedType_Property ('Audit_ChangedBy')

    public bool IsDeleted { get; set; }

    [NotMapped] // alternatively in OnModelCreating(): modelBuilder.Entity<Audit>().Ignore(a => a.ChangedTime);
    public DateTime? ChangedTime { get; set; }

    public InfoType InfoType { get; set; }
}

[Owned]
public class AuditExtended
{
    public string CreatedBy { get; set; } = null!;

    [NotMapped]
    public DateTime? CreatedTime { get; set; }

    [NotMapped]
    public string Remark { get; set; } = null!;
}
public class Tracker
{
    public int TrackerId { get; set; }

    public string Description { get; set; } = null!;

    public TrackerLocation Location { get; set; } = null!;
}
[Owned]
public class TrackerLocation
{
    public string LocationName { get; set; } = null!;
    public Point Location { get; set; } = null!;
}

public class AuditData // used for CustomSqlPostProcessTest
{
    public int AuditDataId { get; set; }

    public int OriginalTableId { get; set; }
    public string TableName { get; set; } = null!;

    public string PrimaryKeyName { get; set; } = null!;

    public string JsonData { get; set; } = null!;

    public DateTime AuditDate { get; set; }
    public string AuditBy { get; set; } = null!;
    public string AuditAction { get; set; } = null!;

}

// For testing BatchUpdate expressions that use nested queries
public class Parent
{
    public ICollection<Child> Children { get; set; } = null!;
    public decimal CombinedChildBalance { get; set; }
    public string Description { get; set; } = null!;
    public virtual ParentDetail Details { get; set; } = null!;
    public int ParentId { get; set; }

    private string _phoneNumber = null!;
    public string PhoneNumber { get => _phoneNumber; set => _phoneNumber = value; }

    public decimal Value { get; set; }
}

public class ParentDetail
{
    public int ParentDetailId { get; set; }

    public virtual Parent Parent { get; set; } = null!;
    public int ParentId { get; set; }

    public string? Notes { get; set; }
}

public class Child
{
    public int ChildId { get; set; }
    public bool IsEnabled { get; set; }
    public decimal Value { get; set; }

    public virtual Parent Parent { get; set; } = null!;
    public int ParentId { get; set; }
}

public abstract class Log // To Test TPT (TablePerType) - https://docs.microsoft.com/en-us/ef/core/modeling/inheritance#table-per-type-configuration
{
    public int LogId { get; set; }
    public int PersonId { get; set; }
    public int RegBy { get; set; }
    public DateTime CreatedDate { get; set; }
}
public class LogPersonReport : Log
{
    public int ReportId { get; set; }
    public int LogPersonReportTypeId { get; set; }
}

public class AtypicalRowVersionEntity
{
    public Guid Id { get; set; }
    public long RowVersion { get; set; }
    public string SyncDevice { get; set; } = null!;
    public string Name { get; set; } = null!;
}

public class AtypicalRowVersionConverterEntity
{
    public Guid Id { get; set; }
    public long RowVersionConverted { get; set; }
    public string Name { get; set; } = null!;
}

public class Event // CustomPrecision DateTime Test (SqlServer only)
{
    public int EventId { get; set; }

    [Required]
    public string Name { get; set; } = null!;

    public string Description { get; set; } = null!;

    [Column(TypeName = "datetime2(3)")]
    public DateTime TimeCreated { get; set; }

    public DateTime? TimeUpdated { get; set; }
}

public class Archive
{
    public byte[] ArchiveId { get; set; } = null!;
    public string Description { get; set; } = null!;
}

public class Counter
{
    public uint CounterId { get; set; }

    public string? Name { get; set; }
}

public class Source
{
    public int SourceId { get; set; }
    public Status StatusId { get; set; }
    public Type TypeId { get; set; }
}
public enum Status : byte { Init, Changed }
public enum Type : byte { Undefined, Type1, Type2 }

public class Department
{
    public Guid Id { get; set; }
    public string Name { get; set; } = null!;

    public ICollection<Division> Divisions { get; set; } = null!;
}

public class Division
{
    public Guid Id { get; set; }
    public string Name { get; set; } = null!;
    public Guid DepartmentId { get; set; }
    public Department? Department { get; set; }
}

public class PrivateKey
{
    private long Id { get; set; }

    public string Name { get; set; } = null!;
}


public class Customer
{
    public int Id { get; set; }

    public string Name { get; set; } = null!;
}

public class Template
{
    public int Id { get; set; }

    [Column("Name]")] // to test escaping brackets
    public string Name { get; set; } = null!;
}

public class Author
{
    public int Id { get; set; }
    public string Name { get; set; } = null!;
    public ContactDetails Contact { get; set; } = null!;
}

[Owned]
public class ContactDetails
{
    public AddressCD Address { get; set; } = null!;
    public string? Phone { get; set; }
}

[Owned]
public class AddressCD
{
    public AddressCD(string street, string city, string postcode, string country)
    {
        Street = street;
        City = city;
        Postcode = postcode;
        Country = country;
    }

    public string Street { get; set; }
    public string City { get; set; }
    public string Postcode { get; set; }
    public string Country { get; set; }
}

public class Partner
{
    public Guid Id { get; set; }
    public string Name { get; set; } = null!;
    public string? FirstName { get; set; }

    [Timestamp]
    [DatabaseGenerated(DatabaseGeneratedOption.Computed)]
    [Column("xmin", TypeName = "xid")]
    public uint RowVersion { get; set; }
}

public class GraphQLModel
{
    [Key]
    [GraphQLType(typeof(IdType))]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public virtual System.Guid Id { get; set; }

    public string? Name { get; set; }
}

public class Article
{
    [Key]
    [GraphQLType(typeof(IdType))]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public virtual System.Guid Id { get; set; }

    public string? Name { get; set; }
}


public abstract class Entity : Entity<int>
{
    public new int Id
    {
        get => base.Id;
        set => base.Id = value;
    }

    public override string ToString() => $"{GetType().Name} #{Id}";
}

public abstract class Entity<TKey> where TKey: struct
{
    public TKey Id { get; set; }
}

public class TrayType : Entity;

//--------------------------------------------------------
public sealed class Mod
{
    public ModId Id { get; private set; }
    public PlayerId PlayerId { get; private set; }
    public IReadOnlyCollection<ModStat> Stats => _stats;
    private readonly List<ModStat> _stats = [];

    private Mod(ModId id, PlayerId playerId)
    {
        Id = id;
        PlayerId = playerId;
    }

    public static Mod Create(string id, PlayerId playerId, IEnumerable<ModStat> modStat)
    {
        var modId = ModId.Create(id);
        var mod = new Mod(modId, playerId);

        mod.AddStats(modStat);
        return mod;
    }
    private void AddStats(IEnumerable<ModStat> modStats)
    {
        _stats.AddRange(modStats);
    }
}

public sealed class ModStat
{
    public ModId ModId { get; init; }
    public PlayerId PlayerId { get; init; }

    private ModStat(ModId modId, PlayerId playerId)
    {
        ModId = modId;
        PlayerId = playerId;
    }

    public static ModStat Create(string modId, PlayerId playerId)
    {
        var mId = ModId.Create(modId);
        return new ModStat(mId, playerId);
    }
}
public sealed class PlayerId
{
    public string Value;
    private PlayerId(string value)
        => Value = value;

    public static PlayerId Create(string value)
        => new PlayerId(value);
}
public sealed class ModId
{
    public string Value;
    private ModId(string value)
        => Value = value;

    public static ModId Create(string value)
        => new ModId(value);
}
//--------------------------------------------------------
