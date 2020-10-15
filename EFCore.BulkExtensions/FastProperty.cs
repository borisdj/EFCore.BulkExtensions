﻿using System;
using System.Linq.Expressions;
using System.Reflection;

namespace EFCore.BulkExtensions
{
    public class FastProperty
    {
        public FastProperty(PropertyInfo property)
        {
            Property = property;
            InitializeGet();
            InitializeSet();
        }

        private void InitializeSet()
        {
            var instance = Expression.Parameter(typeof(object), "instance");
            var value = Expression.Parameter(typeof(object), "value");

            UnaryExpression instanceCast = (!Property.DeclaringType.IsValueType) ? Expression.TypeAs(instance, Property.DeclaringType) : Expression.Convert(instance, Property.DeclaringType);
            UnaryExpression valueCast = (!Property.PropertyType.IsValueType) ? Expression.TypeAs(value, Property.PropertyType) : Expression.Convert(value, Property.PropertyType);
            SetDelegate = Expression.Lambda<Action<object, object>>(Expression.Call(instanceCast, GetPropertySetMethod(), valueCast), new ParameterExpression[] { instance, value }).Compile();

            MethodInfo GetPropertySetMethod()
            {
                return Property.GetSetMethod(true) ?? Property.DeclaringType.GetProperty(Property.Name).GetSetMethod(true);
            }
        }

        private void InitializeGet()
        {
            var instance = Expression.Parameter(typeof(object), "instance");
            UnaryExpression instanceCast = (!Property.DeclaringType.IsValueType) ? Expression.TypeAs(instance, Property.DeclaringType) : Expression.Convert(instance, Property.DeclaringType);
            GetDelegate = Expression.Lambda<Func<object, object>>(Expression.TypeAs(Expression.Call(instanceCast, GetPropertyGetMethod()), typeof(object)), instance).Compile();

            MethodInfo GetPropertyGetMethod()
            {
                return Property.GetGetMethod(true) ?? Property.DeclaringType.GetProperty(Property.Name).GetGetMethod(true);
            }
        }

        public PropertyInfo Property { get; set; }

        public Func<object, object> GetDelegate;

        public Action<object, object> SetDelegate;

        public object Get(object instance) { return GetDelegate(instance); }
        public void Set(object instance, object value) { SetDelegate(instance, value); }
    }
}
