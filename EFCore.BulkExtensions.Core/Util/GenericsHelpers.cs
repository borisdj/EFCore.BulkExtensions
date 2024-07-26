using System;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions;

/// <summary>
/// This class helps to extract properties of the incoming type which have default sql values
/// </summary>
internal static class GenericsHelpers
{
    internal static IEnumerable<string> GetPropertiesDefaultValue<T>(this T value, Type type, TableInfo tableInfo) where T : class
    {
        // type not obtained from typeof(T) but sent as arg. for IncludeGraph in which case it's not declared the same way
        // Obtain all fields with type pointer.

        var arrayPropertyInfos = type.GetProperties();
        var result = new List<string>();
        foreach (var field in arrayPropertyInfos)
        {
            if (field.GetIndexParameters().Any()) // Skip Indexer: public string this[string pPropertyName] => string.Empty;
            {
                continue;
            }
            var name = field.Name;
            if (!tableInfo.PropertyColumnNamesDict.ContainsKey(name)) // skip non-EF properties
            {
                continue;
            }

            var temp = field.GetValue(value);
            object? defaultValue = null;

            //bypass instance creation if incoming type is an interface or class does not have parameterless constructor
            var hasParameterlessConstructor = type.GetConstructor(Type.EmptyTypes) != null;
            if (!type.IsInterface && hasParameterlessConstructor)
                defaultValue = field.GetValue(Activator.CreateInstance(type, true));

            if (temp == defaultValue ||
                (temp != null && defaultValue != null &&
                 temp.ToString() == "0" && defaultValue.ToString() == "0") || // situation for int/long prop with HasSequence (test DefaultValues: Document)
                (temp is Guid guid && guid == Guid.Empty))
            {
                result.Add(name);
            }
        }

        return result;
    }

    internal static IEnumerable<string>? GetPropertiesWithDefaultValue<T>(this IEnumerable<T> values, Type type, TableInfo tableInfo) where T : class
    {
        //var result = values.SelectMany(x => x.GetPropertiesDefaultValue(type)).ToList().Distinct(); // TODO: Check all options(ComputedAndDefaultValuesTest) and consider optimisation
        var result = values.FirstOrDefault()?.GetPropertiesDefaultValue(type, tableInfo)?.Distinct();
        return result;
    }
}
