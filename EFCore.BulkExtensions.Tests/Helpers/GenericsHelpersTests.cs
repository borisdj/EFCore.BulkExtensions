using System;
using System.Collections;
using System.Collections.Generic;
using EFCore.BulkExtensions.Tests.ValueConverters;
using Xunit;

namespace EFCore.BulkExtensions.Tests.Helpers;

public class GenericsHelpersTests
{
    [Theory]
    [ClassData(typeof(GenericsHelpersTestsData))]
    public void CheckDefaultValue(object? value, bool result)
    {
        Assert.Equal(GenericsHelpers.IsDefaultValue(value), result);
    }

    private class GenericsHelpersTestsData : IEnumerable<object?[]>
    {
        public IEnumerator<object?[]> GetEnumerator()
        {
            yield return [0, true];
            yield return [1, false];
            yield return [-1, false];
            yield return [0f, true];
            yield return [-1.0f, false];
            yield return [0d, true];
            yield return [-1.0d, false];
            yield return [0m, true];
            yield return [-1.0m, false];
            yield return [null, true];
            yield return ["", false];
            yield return [' ', false];
            yield return [false, true];
            yield return [true, false];
            yield return [Guid.Empty, true];
            yield return [Guid.NewGuid(), false];
            yield return [new DateTime(), true];
            yield return [new DateTime(2000, 1, 1), false];
            yield return [new DateOnly(), true];
            yield return [new DateOnly(2000, 1, 1), false];
            yield return [new TimeOnly(), true];
            yield return [new TimeOnly(1, 0), false];
            yield return [new DateTimeOffset(), true];
            yield return [new DateTimeOffset(2000, 1, 1, 1, 1, 1, TimeSpan.Zero), false];
            yield return [new LocalDate(), true];
            yield return [new LocalDate(2000, 1, 1), false];
            yield return [new object(), false];
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
