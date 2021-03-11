using System;
using System.Collections.Generic;
using System.Text;

namespace EFCore.BulkExtensions.Tests.ValueConverters
{
    public class VcModel
    {
        public int Id { get; set; }

        public VcEnum Enum { get; set; }
    }

    public enum VcEnum
    {
        Why,
        Hello,
        There
    }
}
