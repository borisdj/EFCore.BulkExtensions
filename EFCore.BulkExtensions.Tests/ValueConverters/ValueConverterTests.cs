using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests.ValueConverters
{
    public class ValueConverterTests: IDisposable
    {
        [Theory]
        [InlineData(DbServer.SqlServer)]
        [InlineData(DbServer.Sqlite)]
        public void BulkInsertOrUpdate_EntityUsingBuiltInEnumToStringConverter_SavesToDatabase(DbServer databaseType)
        {
            ContextUtil.DbServer = databaseType;

            using var db = new VcDbContext(ContextUtil.GetOptions<VcDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_ValueConverters"));

            db.BulkInsertOrUpdate(this.GetTestData(db).ToList());

            var connection = db.Database.GetDbConnection();
            connection.Open();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = "SELECT * FROM VcModels ORDER BY Id DESC";
            cmd.CommandType = System.Data.CommandType.Text;

            using var reader = cmd.ExecuteReader();
            reader.Read();

            var enumStr = reader.Field<string>("Enum");

            Assert.Equal(VcEnum.Hello.ToString(), enumStr);
        }

        private IEnumerable<VcModel> GetTestData(DbContext db)
        {
            var one = new VcModel();
            one.Enum = VcEnum.Hello;

            yield return one;
        }

        public void Dispose()
        {
            using var db = new VcDbContext(ContextUtil.GetOptions<VcDbContext>(databaseName: $"{nameof(EFCoreBulkTest)}_ValueConverters"));
            db.Database.EnsureDeleted();
        }
    }
}
