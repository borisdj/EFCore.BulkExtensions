using System;
using RT.Comb;

namespace EFCore.BulkExtensions.Tests
{
    public static class SeqGuid
    {
        private static ICombProvider SqlNoRepeatCombs = new SqlCombProvider(new SqlDateTimeStrategy(), new UtcNoRepeatTimestampProvider().GetTimestamp);

        public static Guid Create()
        {
            return SqlNoRepeatCombs.Create();
        }
    }
}
