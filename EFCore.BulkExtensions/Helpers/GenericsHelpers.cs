using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace EFCore.BulkExtensions.Helpers
{
    internal static class GenericsHelpers
    {

        internal static IEnumerable<string> GetPropertiesDefaultValue<T>(this T value) where T : class
        {
            Type type = typeof(T);
            // Obtain all fields with type pointer.
            PropertyInfo[] arrayPropertyInfos = type.GetProperties();
            var result = new List<string>();
            foreach (var field in arrayPropertyInfos)
            {
                string name = field.Name;
                object temp = field.GetValue(value);
                if (temp == default) result.Add(name);
                if(temp is Guid && (Guid)temp == Guid.Empty) result.Add(name);
            }

            return result;
        }

        internal static IEnumerable<string> GetPropertiesWithDefaultValue<T>(this IEnumerable<T> values) where T : class
        {
            var result = values.SelectMany(x => x.GetPropertiesDefaultValue()).ToList().Distinct();

            return result;    
        }

    }
}
