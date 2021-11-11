using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using Xunit;

namespace EFCore.BulkExtensions.Tests.BatchUtil
{
    public class Issue658
    {
        public static IEnumerable<object[]> AllDbServers => Enum.GetValues<DbServer>().Select(v => new object[] { v });

        /// <summary>
        /// Reproduces the root cause of issue #658
        /// </summary>
        [Theory]
        [MemberData(nameof(AllDbServers))]
        public void GetSqlSetSegment_ShouldNotIncludePropertyInSetSegment_WhenValueDidNotChange(DbServer dbServer)
        {
            ContextUtil.DbServer = dbServer;
            
            using var dbContext = new TestContext(ContextUtil.GetOptions<TestContext>(databaseName: $"{nameof(Issue658)}_GetSqlSetSegment"));
            var model = new TestModel { Flag = true };
            var parameters = new List<object>();

            var setSegment = BulkExtensions.BatchUtil.GetSqlSetSegment(dbContext, model.GetType(), model, null, parameters);
            
            AssertPropertyNotMentioned(nameof(model.ConvertedDateTimeOffsetProperty), setSegment, parameters);
            AssertPropertyNotMentioned(nameof(model.ConvertedNullableDateTimeOffsetProperty), setSegment, parameters);
            AssertPropertyNotMentioned(nameof(model.NormalDateTimeOffsetProperty), setSegment, parameters);
            AssertPropertyNotMentioned(nameof(model.NormalNullableDateTimeOffsetProperty), setSegment, parameters);
        }
        
        private static void AssertPropertyNotMentioned(string propertyName, string setSegment, IEnumerable<object> parameters)
        {
            Assert.DoesNotContain(propertyName, setSegment);
            Assert.DoesNotContain(parameters, o => o is IDbDataParameter p && p.ParameterName.Contains(propertyName));
        }

        private class TestContext : DbContext
        {
            public TestContext([NotNull] DbContextOptions options) : base(options)
            {
            }

            protected override void OnModelCreating(ModelBuilder modelBuilder)
            {
                base.OnModelCreating(modelBuilder);

                modelBuilder.Entity<TestModel>(cfg =>
                {
                    cfg.HasKey(m => m.Id);
                    cfg.Property(m => m.Id).UseIdentityColumn();
                    cfg.Property(m => m.Flag);
                    cfg.Property(m => m.ConvertedDateTimeOffsetProperty).HasConversion(new DateTimeOffsetToBinaryConverter());
                    cfg.Property(m => m.ConvertedNullableDateTimeOffsetProperty).HasConversion(new DateTimeOffsetToBinaryConverter());
                    cfg.Property(m => m.NormalDateTimeOffsetProperty);
                    cfg.Property(m => m.NormalNullableDateTimeOffsetProperty);
                });
            }
        }
        
        private class TestModel
        {
            public int Id { get; set; }
            public bool Flag { get; set; }
            public DateTimeOffset ConvertedDateTimeOffsetProperty { get; set; }
            public DateTimeOffset? ConvertedNullableDateTimeOffsetProperty { get; set; }
            public DateTimeOffset NormalDateTimeOffsetProperty { get; set; }
            public DateTimeOffset? NormalNullableDateTimeOffsetProperty { get; set; }
        }
    }
}
