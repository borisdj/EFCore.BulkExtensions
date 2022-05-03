using System.Collections;
using System.Collections.Generic;

namespace EFCore.BulkExtensions.Tests.IncludeGraph.Model;

public class Asset
{
    public int Id { get; set; }
    public int? ParentAssetId { get; set; }

    public string Description { get; set; }
    public string Location { get; set; }

    public Asset ParentAsset { get; set; }
    public ICollection<Asset> ChildAssets { get; set; } = new HashSet<Asset>();
    public ICollection<WorkOrder> WorkOrders { get; set; } = new HashSet<WorkOrder>();
    
}
