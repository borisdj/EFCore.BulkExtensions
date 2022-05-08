namespace EFCore.BulkExtensions.Tests.IncludeGraph.Model;

public class WorkOrderSpare
{
    public int Id { get; set; }
    public string Description { get; set; } = null!;
    public decimal Quantity { get; set; }

    public WorkOrder WorkOrder { get; set; } = null!;
    public Spare Spare { get; set; } = null!;
}
