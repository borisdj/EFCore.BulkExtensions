using EFCore.BulkExtensions.SqlAdapters;
using RT.Comb;
using System;

namespace EFCore.BulkExtensions.Tests;

public static class SeqGuid
{
    private static readonly ICombProvider SqlNoRepeatCombs = new SqlCombProvider(new SqlDateTimeStrategy(), new UtcNoRepeatTimestampProvider().GetTimestamp);

    private static readonly ICombProvider UnixCombs = new SqlCombProvider(new UnixDateTimeStrategy(), new UtcNoRepeatTimestampProvider().GetTimestamp);

    private static readonly ICombProvider PGCombs = new PostgreSqlCombProvider(new UnixDateTimeStrategy(), new UtcNoRepeatTimestampProvider().GetTimestamp);

    public static Guid Create(SqlType sqlType = SqlType.SqlServer)
    {
        if(sqlType == SqlType.SqlServer)
            return SqlNoRepeatCombs.Create();
        else if (sqlType == SqlType.PostgreSql)
            return PGCombs.Create();
        else //if (sqlType == SqlType.MySql || sqlType == SqlType.Sqlite)
            return UnixCombs.Create();
    }
}
