using System;
using System.Collections.Generic;
using System.Text;

namespace EFCore.BulkExtensions.Tests.ValueConverters
{
    public class VcModel
    {
        public int Id { get; set; }

        public VcEnum Enum { get; set; }

        public LocalDate LocalDate { get; set; }
    }

    public enum VcEnum
    {
        Why,
        Hello,
        There
    }

    public readonly struct LocalDate
    {
        public LocalDate(int year, int month, int day)
        {
            this.Year = year;
            this.Month = month;
            this.Day = day;
        }

        public readonly int Year;
        public readonly int Month;
        public readonly int Day;

        public static bool operator >(LocalDate lhs, LocalDate rhs)
        {
            return false;
        }

        public static bool operator <(LocalDate lhs, LocalDate rhs)
        {
            return false;
        }
    }
}
