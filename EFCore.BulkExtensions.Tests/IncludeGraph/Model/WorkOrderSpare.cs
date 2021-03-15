using System;
using System.Collections.Generic;
using System.Text;

namespace EFCore.BulkExtensions.Tests.IncludeGraph.Model
{
    public class WorkOrderSpare
    {
        public int Id { get; set; }
        public string Description { get; set; }
        public decimal Quantity { get; set; }

        public WorkOrder WorkOrder { get; set; }
        public Spare Spare { get; set; }
    }
}
