using System;
using System.Linq.Expressions;
using System.Reflection;

namespace EFCore.BulkExtensions;

/// <summary>
/// Class to initialize types using reflection
/// </summary>
public class FastProperty
{
    /// <summary>
    /// Constructor for FastPropery
    /// </summary>
    /// <param name="property"></param>
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
        var setter = Property.GetSetMethod(true) ?? Property.DeclaringType.GetProperty(Property.Name).GetSetMethod(true); // when Prop from parent it requires DeclaringType
        if(setter != null)
            SetDelegate = Expression.Lambda<Action<object, object>>(Expression.Call(instanceCast, setter, valueCast), new ParameterExpression[] { instance, value }).Compile();
    }

    private void InitializeGet()
    {
        var instance = Expression.Parameter(typeof(object), "instance");
        UnaryExpression instanceCast = (!Property.DeclaringType.IsValueType) ? Expression.TypeAs(instance, Property.DeclaringType) : Expression.Convert(instance, Property.DeclaringType);
        var getter = Property.GetGetMethod(true) ?? Property.DeclaringType.GetProperty(Property.Name).GetGetMethod(true);
        if (getter != null)
            GetDelegate = Expression.Lambda<Func<object, object>>(Expression.TypeAs(Expression.Call(instanceCast, getter), typeof(object)), instance).Compile();
    }

#pragma warning disable CS1591 // No XML comment required here
    public PropertyInfo Property { get; set; }

    public Func<object, object> GetDelegate;

    public Action<object, object> SetDelegate;

#pragma warning restore CS1591 // No XML comment required here

    /// <summary>
    /// Returns the object
    /// </summary>
    /// <param name="instance"></param>
    /// <returns></returns>
    public object Get(object instance) { return instance == default ? default : GetDelegate(instance); }

    /// <summary>
    /// Sets the delegate
    /// </summary>
    /// <param name="instance"></param>
    /// <param name="value"></param>
    public void Set(object instance, object value)
    {
        if(value != default)
        {
            SetDelegate(instance, value);
        }
    }
}
