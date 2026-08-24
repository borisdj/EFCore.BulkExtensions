using System;
using System.Collections.Generic;
using System.Text;

namespace EFCore.BulkExtensions.GaussDB.Extensions;

/// <summary>
/// Provides GaussDB-specific bulk configuration extensions.
/// </summary>
public static class BulkConfigExtensions
{
    /// <summary>
    /// Returns the bulk configuration for use with the GaussDB provider.
    /// </summary>
    /// <param name="config">The configuration to use.</param>
    /// <returns>The provided configuration.</returns>
    public static BulkConfig UseGaussDB(this BulkConfig config)
    {
        return config;
    }
}
