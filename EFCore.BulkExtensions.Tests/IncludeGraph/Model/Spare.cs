using System;
using System.Collections.Generic;
using System.Text;

namespace EFCore.BulkExtensions.Tests.IncludeGraph.Model
{
    public class Spare
    {
        public int Id { get; set; }

        public string PartNumber { get; set; }
        public string Barcode { get; set; }
    }
}
