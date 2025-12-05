using System;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// Provides access for Progress info
/// </summary>
public static class ProgressHelper
{
    /// <summary>
    /// For setting Progress data
    /// </summary>
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

    /// <summary>
    /// For getting Progress data
    /// </summary>
    public static decimal GetProgress(int entitiesCount, long rowsCopied)
    {
        return (decimal)(Math.Floor(rowsCopied * 10000D / entitiesCount) / 10000);
    }
}
