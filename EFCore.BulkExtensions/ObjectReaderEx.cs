using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using FastMember;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EFCore.BulkExtensions
{
    internal class ObjectReaderEx : ObjectReader // Overridden to fix ShadowProperties in FastMember library
    {
        private readonly HashSet<string> _shadowProperties;
        private readonly Dictionary<string, ValueConverter> _convertibleProperties;
        private readonly DbContext _context;
        private string[] _members;
        private FieldInfo _current;

        public ObjectReaderEx(Type type, IEnumerable source, HashSet<string> shadowProperties, Dictionary<string, ValueConverter> convertibleProperties, DbContext context, params string[] members) : base(type, source, members)
        {
            _shadowProperties = shadowProperties;
            _convertibleProperties = convertibleProperties;
            _context = context;
            _members = members;
            _current = typeof(ObjectReader).GetField("current", BindingFlags.Instance | BindingFlags.NonPublic);
        }

        public static ObjectReader Create<T>(IEnumerable<T> source, HashSet<string> shadowProperties, Dictionary<string, ValueConverter> convertibleProperties, DbContext context, params string[] members)
        {
            bool hasShadowProp = shadowProperties.Count > 0;
            bool hasConvertibleProperties = convertibleProperties.Keys.Count > 0;
            return (hasShadowProp || hasConvertibleProperties) ? (ObjectReader)new ObjectReaderEx(typeof(T), source, shadowProperties, convertibleProperties, context, members) : ObjectReader.Create(source, members);
        }

        public override object this[string name]
        {
            get
            {
                if (_shadowProperties.Contains(name))
                {
                    var current = _current.GetValue(this);
                    return _context.Entry(current).Property(name).CurrentValue;
                }
                else if (_convertibleProperties.TryGetValue(name, out var converter))
                {
                    var current = _current.GetValue(this);
                    var currentValue = _context.Entry(current).Property(name).CurrentValue;
                    return converter.ConvertToProvider(currentValue);
                }
                return base[name];
            }
        }

        public override object this[int i]
        {
            get
            {
                var name = _members[i];
                return this[name];
            }
        }
    }
}
