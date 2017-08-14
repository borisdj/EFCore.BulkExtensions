using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using FastMember;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    internal class ObjectReaderEx : ObjectReader
    {
        private readonly HashSet<string> _shadowProperties;
        private readonly DbContext _context;
        private string[] _members;
        private static TypeAccessor _accessor = TypeAccessor.Create(typeof(ObjectReader), true);
        private FieldInfo _current;

        internal static object GetInstanceField(Type type, object instance, string fieldName)
        {
            FieldInfo field = type.GetField("current", BindingFlags.Instance | BindingFlags.NonPublic);
            return field.GetValue(instance);
        }

        public ObjectReaderEx(Type type, IEnumerable source, HashSet<string> shadowProperties, DbContext context, params string[] members) : base(type, source, members)
        {
            _shadowProperties = shadowProperties;
            _context = context;
            _members = members;
            _current = typeof(ObjectReader).GetField("current", BindingFlags.Instance | BindingFlags.NonPublic);
        }

        public static ObjectReader Create<T>(IEnumerable<T> source, HashSet<string> shadowProperties, DbContext context, params string[] members)
        {
            bool hasShadowProp = shadowProperties.Count > 0;
            return hasShadowProp ? (ObjectReader)new ObjectReaderEx(typeof(T), source, shadowProperties, context, members) : ObjectReader.Create(source, members);
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
