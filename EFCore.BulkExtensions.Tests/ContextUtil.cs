using System;
using System.Collections.Generic;
using System.Linq;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.Extensions.Configuration;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure.Internal;

namespace EFCore.BulkExtensions.Tests;

public class ContextUtil
{
    public ContextUtil(SqlType sqlType)
    {
        SqlType = sqlType;
    }

    /// <summary>
    /// Type of the database server
    /// </summary>
    public SqlType SqlType { get; }

    public DbContextOptions GetOptions(IInterceptor dbInterceptor) => GetOptions([dbInterceptor]);
    public DbContextOptions GetOptions(IEnumerable<IInterceptor>? dbInterceptors = null) => GetOptions<TestContext>(dbInterceptors);

    public DbContextOptions GetOptions<TDbContext>(IEnumerable<IInterceptor>? dbInterceptors = null, string databaseName = nameof(EFCoreBulkTest))
        where TDbContext : DbContext
        => GetOptions<TDbContext>(SqlType, dbInterceptors, databaseName);

    public DbContextOptions GetOptions<TDbContext>(SqlType dbServerType, 
        IEnumerable<IInterceptor>? dbInterceptors = null, 
        string databaseName = nameof(EFCoreBulkTest))
        where TDbContext : DbContext
    {
        var optionsBuilder = new DbContextOptionsBuilder<TDbContext>();
        
        switch (dbServerType)
        {
            case SqlType.SqlServer:
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
                    opt.CommandTimeout(120);
                });
                break;
            }
            case SqlType.PostgreSql:
            {
                string connectionString = GetPostgreSqlConnectionString(databaseName);
#if NET8_0
                optionsBuilder.UseNpgsql(connectionString);
#else
                var dataSource = new NpgsqlDataSourceBuilder(connectionString)
                    .EnableDynamicJson()
                    .UseNetTopologySuite()
                    .Build();
                optionsBuilder.UseNpgsql(dataSource/*, opt => opt.UseNetTopologySuite()*/);
#endif
                break;
            }
            /*case SqlType.MySql:
            {
                string connectionString = GetMySqlConnectionString(databaseName);
                optionsBuilder.UseMySql(connectionString, ServerVersion.AutoDetect(connectionString), opt => opt.UseNetTopologySuite());
                break;
            }*/
            case SqlType.Oracle:
            {
                string connectionString = GetOracleConnectionString(databaseName);
                optionsBuilder.UseOracle(connectionString);
                break;
            }
            /*case SqlType.GBase:
            {
                string connectionString = GetGBaseConnectionString(databaseName);
                optionsBuilder.UseGBase(connectionString);
                break;
            }*/
            case SqlType.Sqlite:
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
                break;
            }
            default:
                throw new NotSupportedException($"Database {dbServerType} is not supported");
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
        return GetConnectionString("SqlServer").Replace("{databaseName}", databaseName);
    }

    public static string GetSqliteConnectionString(string databaseName)
    {
        return GetConnectionString("Sqlite").Replace("{databaseName}", databaseName);
    }

    public static string GetPostgreSqlConnectionString(string databaseName)
    {
        return GetConnectionString("PostgreSql").Replace("{databaseName}", databaseName);
    }

    public static string GetMySqlConnectionString(string databaseName)
    {
        return GetConnectionString("MySql").Replace("{databaseName}", databaseName);
    }

    public static string GetOracleConnectionString(string databaseName)
    {
        return GetConnectionString("Oracle").Replace("{databaseName}", databaseName);
    }
    public static string GetGBaseConnectionString(string databaseName)
    {
        return GetConnectionString("GBase").Replace("{databaseName}", databaseName);
    }

    private static string GetConnectionString(string name)
    {
        return GetConfiguration().GetConnectionString(name) ?? throw new Exception($"Connection string '{name}' not found.");
    }
}
