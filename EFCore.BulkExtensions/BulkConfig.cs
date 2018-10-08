using System.Collections.Generic;
using System.Data.SqlClient;

namespace EFCore.BulkExtensions
{
    public class BulkConfig
    {
        public int BatchSize { get; set; } = 2000;

        public int? BulkCopyTimeout { get; set; }

        public bool EnableStreaming { get; set; }

        public bool GenerateStats { get; set; }

        public int? NotifyAfter { get; set; }

        public bool PreserveInsertOrder { get; set; }

        public List<string> PropertiesToExclude { get; set; }

        public List<string> PropertiesToInclude { get; set; }

        public bool SetOutputIdentity { get; set; }

        public SqlBulkCopyOptions SqlBulkCopyOptions { get; set; }

        public List<string> UpdateByProperties { get; set; }

        public bool UseTempDB { get; set; }

        public bool WithHoldlock { get; set; } = true;
    }
}
