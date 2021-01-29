using System;
using System.Collections.Generic;
using System.Data.Common;

namespace EFCore.BulkExtensions
{
    public class BulkConfig
    {
        public bool PreserveInsertOrder { get; set; }

        public bool SetOutputIdentity { get; set; }

        public int BatchSize { get; set; } = 2000;

        public int? NotifyAfter { get; set; }

        public int? BulkCopyTimeout { get; set; }

        public bool EnableStreaming { get; set; }

        public bool UseTempDB { get; set; }

        public bool UniqueTableNameTempDb { get; set; } = true;

        public bool TrackingEntities { get; set; }

        public bool WithHoldlock { get; set; } = true;

        public bool CalculateStats { get; set; }
        public StatsInfo StatsInfo { get; set; }

        public List<string> PropertiesToInclude { get; set; }

        public List<string> PropertiesToExclude { get; set; }

        public List<string> UpdateByProperties { get; set; }

        // since Microsoft.Data.SqlClient.SqlBulkCopyOptions is a superset of 
        // System.Data.SqlClient.SqlBulkCopyOptions, allow user to always specify
        // Microsoft.Data.SqlClient and we will convert it to the desired type
        public Microsoft.Data.SqlClient.SqlBulkCopyOptions SqlBulkCopyOptions { get; set; }

        public Func<DbConnection, DbConnection> UnderlyingConnection { get; set; }
        public Func<DbTransaction, DbTransaction> UnderlyingTransaction { get; set; }

        internal OperationType OperationType { get; set; }
    }

    public class StatsInfo
    {
        public int StatsNumberInserted { get; set; }

        public int StatsNumberUpdated { get; set; }

        public int StatsNumberDeleted { get; set; }
    }
}
