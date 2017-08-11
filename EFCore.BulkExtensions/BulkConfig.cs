namespace EFCore.BulkExtensions
{
    public class BulkConfig
    {
        public bool PreserveInsertOrder { get; set; } = false;

        public bool SetOutputIdentity { get; set; } = false;

        public int BatchSize { get; set; } = 2000;
    }
}
