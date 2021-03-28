using System;
using System.Collections.Generic;
using System.Data.Common;

namespace EFCore.BulkExtensions
{
    public class BulkConfig
    {
        /// <summary>
        ///     Makes sure that entites are inserted to Db as ordered in entitiesList.
        ///     Is <c>true</c> by default.
        /// </summary>
        public bool PreserveInsertOrder { get; set; } = true;

        /// <summary>
        ///     ToDo
        /// </summary>
        public bool SetOutputIdentity { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public int BatchSize { get; set; } = 2000;

        /// <summary>
        ///     ToDo
        /// </summary>
        public int? NotifyAfter { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public int? BulkCopyTimeout { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public bool EnableStreaming { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public bool UseTempDB { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public bool UniqueTableNameTempDb { get; set; } = true;

        /// <summary>
        ///     ToDo
        /// </summary>
        public string CustomDestinationTableName { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public bool TrackingEntities { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public bool WithHoldlock { get; set; } = true;

        /// <summary>
        ///     ToDo
        /// </summary>
        public bool CalculateStats { get; set; }
        /// <summary>
        ///     ToDo
        /// </summary>
        public StatsInfo StatsInfo { get; set; }
        /// <summary>
        ///     ToDo
        /// </summary>
        public TimeStampInfo TimeStampInfo { get; internal set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public List<string> PropertiesToInclude { get; set; }
        /// <summary>
        ///     ToDo
        /// </summary>
        public List<string> PropertiesToIncludeOnCompare { get; set; }
        /// <summary>
        ///     ToDo
        /// </summary>
        public List<string> PropertiesToIncludeOnUpdate { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public List<string> PropertiesToExclude { get; set; }
        /// <summary>
        ///     ToDo
        /// </summary>
        public List<string> PropertiesToExcludeOnCompare { get; set; }
        /// <summary>
        ///     ToDo
        /// </summary>
        public List<string> PropertiesToExcludeOnUpdate { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public List<string> UpdateByProperties { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public bool EnableShadowProperties { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public bool IncludeGraph { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public bool SkipClauseExistsExcept { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public bool DoNotUpdateIfTimeStampChanged { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public int SRID { get; set; } = 4326; // Spatial Reference Identifier // https://docs.microsoft.com/en-us/sql/relational-databases/spatial/spatial-reference-identifiers-srids

        /// <summary>
        ///     ToDo
        /// </summary>
        public Microsoft.Data.SqlClient.SqlBulkCopyOptions SqlBulkCopyOptions { get; set; } // is superset of System.Data.SqlClient.SqlBulkCopyOptions, gets converted to the desired type

        /// <summary>
        ///     ToDo
        /// </summary>
        public Func<DbConnection, DbConnection> UnderlyingConnection { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public Func<DbTransaction, DbTransaction> UnderlyingTransaction { get; set; }

        internal OperationType OperationType { get; set; }
    }

    public class StatsInfo
    {
        /// <summary>
        ///     ToDo
        /// </summary>
        public int StatsNumberInserted { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>

        public int StatsNumberUpdated { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>

        public int StatsNumberDeleted { get; set; }
    }

    public class TimeStampInfo
    {
        /// <summary>
        ///     ToDo
        /// </summary>
        public int NumberOfSkippedForUpdate { get; set; }

        /// <summary>
        ///     ToDo
        /// </summary>
        public List<object> EntitiesOutput { get; set; }
    }
}
