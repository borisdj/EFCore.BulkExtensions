using System;
using System.Collections.Generic;
using System.Text;

namespace EFCore.BulkExtensions.Tests.IncludeGraph.Model
{
    public class Asset
    {
        public int Id { get; set; }
        public string Description { get; set; }
        public string Location { get; set; }
    }
}
