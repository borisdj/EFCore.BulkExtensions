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
    }
}
