using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Caching.Memory;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFCoreBulkUnderlyingTest
    {
        protected int EntitiesNumber => 100000;

        private static Func<TestContext, int> ItemsCountQuery = EF.CompileQuery<TestContext, int>(ctx => ctx.Items.Count());
        private static Func<TestContext, Item> LastItemQuery = EF.CompileQuery<TestContext, Item>(ctx => ctx.Items.LastOrDefault());
        private static Func<TestContext, IEnumerable<Item>> AllItemsQuery = EF.CompileQuery<TestContext, IEnumerable<Item>>(ctx => ctx.Items.AsNoTracking());

        [Theory]
        [InlineData(true)]
        //[InlineData(false)] // for speed comparison with Regular EF CUD operations
        public void OperationsTest(bool isBulkOperation)
        {
            //DeletePreviousDatabase();

            RunInsert(isBulkOperation);
            RunInsertOrUpdate(isBulkOperation);
            RunUpdate(isBulkOperation);
            RunRead(isBulkOperation);
            RunDelete(isBulkOperation);

            CheckQueryCache();
        }

        private void DeletePreviousDatabase()
        {
            using (var context = new TestContext(GetOptions()))
            {
                context.Database.EnsureDeleted();
            }
        }

        private void CheckQueryCache()
        {
            using (var context = new TestContext(GetOptions()))
            {
                var compiledQueryCache = ((MemoryCache)context.GetService<IMemoryCache>());

                Assert.Equal(0, compiledQueryCache.Count);
            }
        }

        private void WriteProgress(decimal percentage)
        {
            Debug.WriteLine(percentage);
        }

        public static DbContextOptions GetOptions()
        {
            var builder = new DbContextOptionsBuilder<TestContext>();
            var databaseName = nameof(EFCoreBulkTest);
            var connectionString = $"Server=.\\SQLEXPRESS;Database={databaseName};Trusted_Connection=True;MultipleActiveResultSets=true";
            //builder.UseSqlServer(connectionString);
            var connection = new SqlConnection(connectionString) as DbConnection;
            connection = new MyConnection(connection);
            builder.UseSqlServer(connection); // Can NOT Test with UseInMemoryDb (Exception: Relational-specific methods can only be used when the context is using a relational)
            return builder.Options;
        }


        private void RunInsert(bool isBulkOperation)
        {
            using (var context = new TestContext(GetOptions()))
            {
                var entities = new List<Item>();
                var subEntities = new List<ItemHistory>();
                for (int i = 1; i < EntitiesNumber; i++)
                {
                    var entity = new Item
                    {
                        ItemId = isBulkOperation ? i : 0,
                        Name = "name " + i,
                        Description = "info " + Guid.NewGuid().ToString().Substring(0, 3),
                        Quantity = i % 10,
                        Price = i / (i % 5 + 1),
                        TimeUpdated = DateTime.Now,
                        ItemHistories = new List<ItemHistory>()
                    };

                    var subEntity1 = new ItemHistory
                    {
                        ItemHistoryId = SeqGuid.Create(),
                        Remark = $"some more info {i}.1"
                    };
                    var subEntity2 = new ItemHistory
                    {
                        ItemHistoryId = SeqGuid.Create(),
                        Remark = $"some more info {i}.2"
                    };
                    entity.ItemHistories.Add(subEntity1);
                    entity.ItemHistories.Add(subEntity2);

                    entities.Add(entity);
                }

                if (isBulkOperation)
                {
                    using (var transaction = context.Database.BeginTransaction())
                    {
                        context.BulkInsert(
                            entities,
                            new BulkConfig
                            {
                                PreserveInsertOrder = true,
                                SetOutputIdentity = true,
                                BatchSize = 4000,
                                UseTempDB = true,
                                GetUnderlyingConnection = GetUnderlyingConnection,
                                GetUnderlyingTransaction = GetUnderlyingTransaction
                            },
                            (a) => WriteProgress(a)
                        );

                        foreach (var entity in entities)
                        {
                            foreach (var subEntity in entity.ItemHistories)
                            {
                                subEntity.ItemId = entity.ItemId; // setting FK to match its linked PK that was generated in DB
                            }
                            subEntities.AddRange(entity.ItemHistories);
                        }
                        context.BulkInsert(subEntities, new BulkConfig()
                        {
                            GetUnderlyingConnection = GetUnderlyingConnection, GetUnderlyingTransaction = GetUnderlyingTransaction
                        });

                        transaction.Commit();
                    }
                }
                else
                {
                    context.Items.AddRange(entities);
                    context.SaveChanges();
                }
            }

            using (var context = new TestContext(GetOptions()))
            {
                int entitiesCount = ItemsCountQuery(context);
                Item lastEntity = LastItemQuery(context);

                Assert.Equal(EntitiesNumber - 1, entitiesCount);
                Assert.NotNull(lastEntity);
                Assert.Equal("name " + (EntitiesNumber - 1), lastEntity.Name);
            }
        }

        private void RunInsertOrUpdate(bool isBulkOperation)
        {
            using (var context = new TestContext(GetOptions()))
            {
                var entities = new List<Item>();
                var dateTimeNow = DateTime.Now;
                for (int i = 2; i <= EntitiesNumber; i += 2)
                {
                    entities.Add(new Item
                    {
                        ItemId = isBulkOperation ? i : 0,
                        Name = "name InsertOrUpdate " + i,
                        Description = "info",
                        Quantity = i + 100,
                        Price = i / (i % 5 + 1),
                        TimeUpdated = dateTimeNow
                    });
                }
                if (isBulkOperation)
                {
                    var bulkConfig = new BulkConfig() { SetOutputIdentity = true, CalculateStats = true, GetUnderlyingConnection = GetUnderlyingConnection, GetUnderlyingTransaction = GetUnderlyingTransaction};
                    context.BulkInsertOrUpdate(entities, bulkConfig, (a) => WriteProgress(a));
                }
                else
                {
                    context.Items.Add(entities[entities.Count - 1]);
                    context.SaveChanges();
                }
            }
            using (var context = new TestContext(GetOptions()))
            {
                int entitiesCount = ItemsCountQuery(context);
                Item lastEntity = LastItemQuery(context);

                Assert.Equal(EntitiesNumber, entitiesCount);
                Assert.NotNull(lastEntity);
                Assert.Equal("name InsertOrUpdate " + EntitiesNumber, lastEntity.Name);
            }
        }

        private void RunUpdate(bool isBulkOperation)
        {
            using (var context = new TestContext(GetOptions()))
            {
                int counter = 1;
                var entities = AllItemsQuery(context).ToList(); // context.Items.AsNoTracking());
                foreach (var entity in entities)
                {
                    entity.Description = "Desc Update " + counter++;
                    entity.Quantity = entity.Quantity + 1000; // will not be changed since Quantity property is not in config PropertiesToInclude
                }
                if (isBulkOperation)
                {
                    context.BulkUpdate(
                        entities,
                        new BulkConfig
                        {
                            PropertiesToInclude = new List<string> { nameof(Item.Description) },
                            UpdateByProperties = new List<string> { nameof(Item.Name) }
                            , GetUnderlyingConnection = GetUnderlyingConnection, GetUnderlyingTransaction = GetUnderlyingTransaction
                        }
                    );
                }
                else
                {
                    context.Items.UpdateRange(entities);
                    context.SaveChanges();
                }
            }
            using (var context = new TestContext(GetOptions()))
            {
                int entitiesCount = ItemsCountQuery(context);
                Item lastEntity = LastItemQuery(context);

                Assert.Equal(EntitiesNumber, entitiesCount);
                Assert.NotNull(lastEntity);
                Assert.Equal("name InsertOrUpdate " + EntitiesNumber, lastEntity.Name);
            }
        }

        private void RunRead(bool isBulkOperation)
        {
            using (var context = new TestContext(GetOptions()))
            {
                var entities = new List<Item>();

                for (int i = 1; i < EntitiesNumber; i++)
                {
                    var entity = new Item
                    {
                        Name = "name " + i,
                    };
                    entities.Add(entity);
                }

                context.BulkRead(
                    entities,
                    new BulkConfig
                    {
                        UseTempDB = false,
                        UpdateByProperties = new List<string> { nameof(Item.Name) }
                        , GetUnderlyingConnection = GetUnderlyingConnection, GetUnderlyingTransaction = GetUnderlyingTransaction
                    }
                );

                Assert.Equal(1, entities[0].ItemId);
                Assert.Equal(0, entities[1].ItemId);
                Assert.Equal(3, entities[2].ItemId);
                Assert.Equal(0, entities[3].ItemId);
            }
        }

        private void RunDelete(bool isBulkOperation)
        {
            using (var context = new TestContext(GetOptions()))
            {
                var entities = AllItemsQuery(context).ToList();
                // ItemHistories will also be deleted because of Relationship - ItemId (Delete Rule: Cascade)
                if (isBulkOperation)
                {
                    context.BulkDelete(entities, new BulkConfig()
                    {
                        GetUnderlyingConnection = GetUnderlyingConnection,
                        GetUnderlyingTransaction =  GetUnderlyingTransaction
                    });
                }
                else
                {
                    context.Items.RemoveRange(entities);
                    context.SaveChanges();
                }
            }
            using (var context = new TestContext(GetOptions()))
            {
                int entitiesCount = ItemsCountQuery(context);
                Item lastEntity = LastItemQuery(context);

                Assert.Equal(0, entitiesCount);
                Assert.Null(lastEntity);
            }

            using (var context = new TestContext(GetOptions()))
            {
                // Resets AutoIncrement
                context.Database.ExecuteSqlCommand("DBCC CHECKIDENT ('dbo.[" + nameof(Item) + "]', RESEED, 0);"); // can NOT use $"...{nameof(Item)..." because it gets parameterized
                //context.Database.ExecuteSqlCommand($"TRUNCATE TABLE {nameof(Item)};"); // can NOT work when there is ForeignKey - ItemHistoryId
            }
        }

        public DbConnection GetUnderlyingConnection(DbConnection connection)
        {
            if (connection is MyConnection mc) return mc.UnderlyingConection;
            return connection;
        }
        public DbTransaction GetUnderlyingTransaction(DbTransaction transaction)
        {
            if (transaction is MyTransaction mt) return mt.UnderlyingTransaction;
            return transaction;
        }
    }

    public class MyConnection : DbConnection
    {
        public MyConnection(DbConnection underlyingConnection)
        {
            this.UnderlyingConection = underlyingConnection;
        }

        public DbConnection UnderlyingConection { get; }
        public override string ConnectionString { get => UnderlyingConection.ConnectionString;
            set => UnderlyingConection.ConnectionString = value;
        }

        public override string Database => UnderlyingConection.Database;

        public override string DataSource => UnderlyingConection.DataSource;

        public override string ServerVersion => UnderlyingConection.ServerVersion;

        public override ConnectionState State => UnderlyingConection.State;

        public override void ChangeDatabase(string databaseName)
        {
            UnderlyingConection.ChangeDatabase(databaseName);
        }

        public override void Close()
        {
            UnderlyingConection.Close();
        }

        public override void Open()
        {
            UnderlyingConection.Open();
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
        {
            return new MyTransaction(UnderlyingConection.BeginTransaction(), this);
        }

        protected override DbCommand CreateDbCommand()
        {
            return new MyCommand(UnderlyingConection.CreateCommand(), this);
        }
    }

    class MyTransaction : DbTransaction
    {
        public MyTransaction(DbTransaction underlyingTransaction, MyConnection connection)
        {
            this.UnderlyingTransaction = underlyingTransaction;
            this.MyConnection = connection;
        }
        public override IsolationLevel IsolationLevel => UnderlyingTransaction.IsolationLevel;

        public DbTransaction UnderlyingTransaction { get; }
        public MyConnection MyConnection { get; }

        protected override DbConnection DbConnection => this.MyConnection;

        public override void Commit()
        {
            this.UnderlyingTransaction.Commit();
        }

        public override void Rollback()
        {
            this.UnderlyingTransaction.Rollback();
        }
    }

    class MyCommand : DbCommand
    {
        public DbCommand UnderlyingCommand { get; set; }
        public MyConnection MyConnection { get; set; }

        public MyCommand(DbCommand underlyingCommand, MyConnection connection)
        {
            this.UnderlyingCommand = underlyingCommand;
            this.MyConnection = connection;
        }

        public override string CommandText { get => this.UnderlyingCommand.CommandText; set => this.UnderlyingCommand.CommandText =value; }
        public override int CommandTimeout { get => this.UnderlyingCommand.CommandTimeout; set => this.UnderlyingCommand.CommandTimeout = value; }
        public override CommandType CommandType { get => this.UnderlyingCommand.CommandType; set => this.UnderlyingCommand.CommandType = value; }
        public override bool DesignTimeVisible { get => this.UnderlyingCommand.DesignTimeVisible; set => this.UnderlyingCommand.DesignTimeVisible = value; }
        public override UpdateRowSource UpdatedRowSource { get => this.UnderlyingCommand.UpdatedRowSource; set => this.UnderlyingCommand.UpdatedRowSource = value; }
        protected override DbConnection DbConnection { get => this.MyConnection; set => this.MyConnection = (MyConnection)value; }

        protected override DbParameterCollection DbParameterCollection => this.UnderlyingCommand.Parameters;

        public MyTransaction MyTransaction { get; set; }

        protected override DbTransaction DbTransaction
        {
            get => MyTransaction;
            set
            {
                this.MyTransaction = (MyTransaction)value;
                UnderlyingCommand.Transaction = this.MyTransaction?.UnderlyingTransaction;
            }
        }

        public override void Cancel()
        {
            this.UnderlyingCommand.Cancel();
        }

        public override int ExecuteNonQuery()
        {
            return UnderlyingCommand.ExecuteNonQuery();
        }

        public override object ExecuteScalar()
        {
            return UnderlyingCommand.ExecuteScalar();
        }

        public override void Prepare()
        {
            UnderlyingCommand.Prepare();
        }

        protected override DbParameter CreateDbParameter()
        {
            return UnderlyingCommand.CreateParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            return UnderlyingCommand.ExecuteReader(behavior);
        }
    }



}
