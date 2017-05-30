using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    public class BulkConfig
    {
        public bool PreserveInsertOrder { get; set; } = false;

        public bool SetOutputIdentity { get; set; } = false;
    }
}
