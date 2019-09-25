using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using Xunit;

namespace EFCore.BulkExtensions.Tests
{
    public class EFCoreBulkUnderlyingTest
    {
        protected int EntitiesNumber => 1000;

        private static Func<TestContext, int> ItemsCountQuery = EF.CompileQuery<TestContext, int>(ctx => ctx.Items.Count());
        private static Func<TestContext, Item> LastItemQuery = EF.CompileQuery<TestContext, Item>(ctx => ctx.Items.LastOrDefault());
        private static Func<TestContext, IEnumerable<Item>> AllItemsQuery = EF.CompileQuery<TestContext, IEnumerable<Item>>(ctx => ctx.Items.AsNoTracking());

        [Theory]
        [InlineData(true)]
        public void OperationsTest(bool isBulkOperation)
        {
            RunInsert(isBulkOperation);
            RunDelete(isBulkOperation);
        }

        public static DbContextOptions GetOptions()
        {
            var builder = new DbContextOptionsBuilder<TestContext>();
            var databaseName = nameof(EFCoreBulkTest);
            var connectionString = $"Server=localhost;Database={databaseName};Trusted_Connection=True;MultipleActiveResultSets=true";
            var connection = new SqlConnection(connectionString) as DbConnection;
            connection = new MyConnection(connection);
            builder.UseSqlServer(connection);
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
                                UnderlyingConnection = GetUnderlyingConnection,
                                UnderlyingTransaction = GetUnderlyingTransaction
                            }
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
                            UnderlyingConnection = GetUnderlyingConnection, UnderlyingTransaction = GetUnderlyingTransaction
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
                        UnderlyingConnection = GetUnderlyingConnection,
                        UnderlyingTransaction = GetUnderlyingTransaction
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
                context.Database.ExecuteSqlRaw("DBCC CHECKIDENT ('dbo.[" + nameof(Item) + "]', RESEED, 0);");
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
        public DbConnection UnderlyingConection { get; }

        public override string Database => UnderlyingConection.Database;
        public override string DataSource => UnderlyingConection.DataSource;
        public override string ServerVersion => UnderlyingConection.ServerVersion;
        public override ConnectionState State => UnderlyingConection.State;

        public override string ConnectionString
        {
            get => UnderlyingConection.ConnectionString;
            set => UnderlyingConection.ConnectionString = value;
        }

        public MyConnection(DbConnection underlyingConnection)
        {
            UnderlyingConection = underlyingConnection;
        }

        public override void ChangeDatabase(string databaseName)
        {
            UnderlyingConection.ChangeDatabase(databaseName);
        }

        public override void Open()
        {
            UnderlyingConection.Open();
        }

        public override void Close()
        {
            UnderlyingConection.Close();
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
        public DbTransaction UnderlyingTransaction { get; }
        public MyConnection MyConnection { get; }

        public override IsolationLevel IsolationLevel => UnderlyingTransaction.IsolationLevel;
        protected override DbConnection DbConnection => MyConnection;

        public MyTransaction(DbTransaction underlyingTransaction, MyConnection connection)
        {
            UnderlyingTransaction = underlyingTransaction;
            MyConnection = connection;
        }

        public override void Commit()
        {
            UnderlyingTransaction.Commit();
        }

        public override void Rollback()
        {
            UnderlyingTransaction.Rollback();
        }
    }

    class MyCommand : DbCommand
    {
        public DbCommand UnderlyingCommand { get; set; }
        public MyConnection MyConnection { get; set; }

        public MyCommand(DbCommand underlyingCommand, MyConnection connection)
        {
            UnderlyingCommand = underlyingCommand;
            MyConnection = connection;
        }

        public override string CommandText { get => UnderlyingCommand.CommandText; set => UnderlyingCommand.CommandText = value; }
        public override int CommandTimeout { get => UnderlyingCommand.CommandTimeout; set => UnderlyingCommand.CommandTimeout = value; }
        public override CommandType CommandType { get => UnderlyingCommand.CommandType; set => UnderlyingCommand.CommandType = value; }
        public override bool DesignTimeVisible { get => UnderlyingCommand.DesignTimeVisible; set => UnderlyingCommand.DesignTimeVisible = value; }
        public override UpdateRowSource UpdatedRowSource { get => UnderlyingCommand.UpdatedRowSource; set => UnderlyingCommand.UpdatedRowSource = value; }
        protected override DbConnection DbConnection { get => MyConnection; set => MyConnection = (MyConnection)value; }

        protected override DbParameterCollection DbParameterCollection => this.UnderlyingCommand.Parameters;

        public MyTransaction MyTransaction { get; set; }

        protected override DbTransaction DbTransaction
        {
            get => MyTransaction;
            set
            {
                MyTransaction = (MyTransaction)value;
                UnderlyingCommand.Transaction = MyTransaction?.UnderlyingTransaction;
            }
        }

        public override void Cancel()
        {
            UnderlyingCommand.Cancel();
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
