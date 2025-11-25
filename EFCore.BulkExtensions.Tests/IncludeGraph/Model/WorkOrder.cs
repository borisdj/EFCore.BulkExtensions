using System.Collections.Generic;

namespace EFCore.BulkExtensions.Tests.IncludeGraph.Model;

public class WorkOrder
{
    public int Id { get; set; }
    public string Description { get; set; } = null!;

    public int? AssetId { get; set; }
    public Asset? Asset { get; set; }
    public ICollection<WorkOrderSpare> WorkOrderSpares { get; set; } = new HashSet<WorkOrderSpare>();
}
