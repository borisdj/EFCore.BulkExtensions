using System;
using System.Collections.Generic;
using System.Text;

namespace EFCore.BulkExtensions
{
    public class OperationStats
    {
        public string ChangeType { get; set; }
        public int Count { get; set; }
    }
}
