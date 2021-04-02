using RT.Comb;
using System;

namespace EFCore.BulkExtensions.Tests
{
    public static class SeqGuid
    {
        private static readonly ICombProvider SqlNoRepeatCombs = new SqlCombProvider(new SqlDateTimeStrategy(), new UtcNoRepeatTimestampProvider().GetTimestamp);

        public static Guid Create()
        {
            return SqlNoRepeatCombs.Create();
        }
    }
}
