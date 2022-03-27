using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests.IncludeGraph
{
    public class Parent
    {
        public int ID { get; set; }

        public string Name { get; set; }

        public ICollection<Child> Children { get; set; }
    }

    public class Child
    {
        public int ID { get; set; }

        public string Name { get; set; }

        public int Age { get; set; }

        public Parent Parent { get; set; }

        public int ParentID { get; set; }

        public override bool Equals(object obj)
        {
            return obj is Child child &&
                    Age == child.Age &&
                    Name == child.Name;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Name, Age);
        }
    }

    public class Issue730DbContext : DbContext
    {
        public DbSet<Parent> Parents { get; set; }
        public DbSet<Child> Children { get; set; }

        public Issue730DbContext(DbContextOptions options) : base(options)
        {
        }
    }

    public class Issue730 : IDisposable
    {
        // Given: A set of parents with varying number of children, where some children
        // are equal to other children 
        private IEnumerable<Parent> GivenParentsWithChildren(int count)
        {
            return Enumerable
                .Range(1,count)
                .Select(x => new Parent() 
                { 
                    Name = x.ToString(),
                    Children = Enumerable
                        .Range(1,x)
                        .Select(y => new Child() 
                        { 
                            Name = y.ToString(), 
                            Age = y
                        })
                        .ToList()
                });
        }

        // Given: A set of parents with varying number of children, where all children
        // are completely unique 
        private IEnumerable<Parent> GivenParentsWithUniqueChildren(int count)
        {
            int age = 1;
            return Enumerable
                .Range(1,count)
                .Select(x => new Parent() 
                { 
                    Name = x.ToString(),
                    Children = Enumerable
                        .Range(1,x)
                        .Select(y => new Child() 
                        { 
                            Name = y.ToString(), 
                            Age = age++
                        })
                        .ToList()
                });
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        public async Task BulkAddParentsWithChildren(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;

            using var db = new Issue730DbContext(ContextUtil.GetOptions<Issue730DbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Issue730"));
            await db.Database.EnsureCreatedAsync();

            // Given: A set of parents with varying number of children, where some children
            // are equal to other children 
            var count = 25;
            var parents = GivenParentsWithChildren(count).ToList();
            var numchildren = parents.Sum(x=>x.Children.Count);

            // When: Adding the parents to the database (using bulk extensions)
            db.BulkInsert(parents,b => b.IncludeGraph = true);

            // Then: The parents are in the database
            var actual = db.Set<Parent>().Count();
            Assert.Equal(count,actual);

            // And: The children are separately in the database as well
            var childrencount = db.Set<Child>().Count();
            Assert.Equal(numchildren,childrencount);           
        }

        [Theory]
        [InlineData(DbServer.SQLServer)]
        public async Task BulkAddParentsWithUniqueChildren(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;

            using var db = new Issue730DbContext(ContextUtil.GetOptions<Issue730DbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Issue730"));
            await db.Database.EnsureCreatedAsync();

            // Given: A set of parents with varying number of children, where some children
            // are equal to other children 
            var count = 25;
            var parents = GivenParentsWithUniqueChildren(count).ToList();
            var numchildren = parents.Sum(x=>x.Children.Count);

            // When: Adding the parents to the database (using bulk extensions)
            db.BulkInsert(parents,b => { b.IncludeGraph = true; b.OmitClauseExistsExcept = true; });

            // Then: The parents are in the database
            var actual = db.Set<Parent>().Count();
            Assert.Equal(count,actual);

            // And: The children are separately in the database as well
            var childrencount = db.Set<Child>().Count();
            Assert.Equal(numchildren,childrencount);
        }

        public void Dispose()
        {
            using var db = new Issue730DbContext(ContextUtil.GetOptions<Issue730DbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_Issue730"));
            db.Database.EnsureDeleted();
        }
    }
}