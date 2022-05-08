using System;
using System.Linq.Expressions;
using System.Reflection;

namespace EFCore.BulkExtensions;

public class FastProperty
{
    private Func<object, object>? _getDelegate;
    private Action<object, object>? _setDelegate;

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

        if (Property.DeclaringType is null)
        {
            throw new ArgumentException("Unable to determine DeclaringType from Property");
        }

        UnaryExpression instanceCast = (!Property.DeclaringType?.IsValueType ?? false)
            ? Expression.TypeAs(instance, Property.DeclaringType!)
            : Expression.Convert(instance, Property.DeclaringType!);

        UnaryExpression valueCast = (!Property.PropertyType.IsValueType)
            ? Expression.TypeAs(value, Property.PropertyType)
            : Expression.Convert(value, Property.PropertyType);

        var setter = Property.GetSetMethod(true) ?? Property.DeclaringType?.GetProperty(Property.Name)?.GetSetMethod(true); // when Prop from parent it requires DeclaringType

        if (setter != null)
            _setDelegate = Expression.Lambda<Action<object, object>>(Expression.Call(instanceCast, setter, valueCast), new ParameterExpression[] { instance, value }).Compile();
    }

    private void InitializeGet()
    {
        var instance = Expression.Parameter(typeof(object), "instance");

        if (Property.DeclaringType is null)
        {
            throw new ArgumentException("Unable to determine DeclaringType from Property");
        }

        UnaryExpression instanceCast = (!Property.DeclaringType.IsValueType)
            ? Expression.TypeAs(instance, Property.DeclaringType)
            : Expression.Convert(instance, Property.DeclaringType);

        var getter = Property.GetGetMethod(true) ?? Property.DeclaringType.GetProperty(Property.Name)?.GetGetMethod(true);

        if (getter != null)
            _getDelegate = Expression.Lambda<Func<object, object>>(Expression.TypeAs(Expression.Call(instanceCast, getter), typeof(object)), instance).Compile();
    }

    public PropertyInfo Property { get; set; }


    public object? Get(object instance) => instance == default || _getDelegate is null ? default : _getDelegate(instance);

    public void Set(object instance, object? value)
    {
        if (value != default && _setDelegate is not null)
        {
            _setDelegate(instance, value);
        }
    }
}
