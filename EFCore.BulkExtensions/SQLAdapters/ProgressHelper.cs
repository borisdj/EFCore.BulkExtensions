using System;

namespace EFCore.BulkExtensions.SqlAdapters;

#pragma warning disable CS1591 // No XML comments required here
public static class ProgressHelper
{
    public static void SetProgress(ref int rowsCopied, int entitiesCount, BulkConfig bulkConfig, Action<decimal>? progress)
    {
        if (progress != null && bulkConfig.NotifyAfter != null && bulkConfig.NotifyAfter != 0)
        {
            rowsCopied++;

            if (rowsCopied == entitiesCount || rowsCopied % bulkConfig.NotifyAfter == 0)
            {
                progress.Invoke(GetProgress(entitiesCount, rowsCopied));
            }
        }
    }

    public  static decimal GetProgress(int entitiesCount, long rowsCopied)
    {
        return (decimal)(Math.Floor(rowsCopied * 10000D / entitiesCount) / 10000);
    }
}
#pragma warning restore CS1591 // No XML comments required here
