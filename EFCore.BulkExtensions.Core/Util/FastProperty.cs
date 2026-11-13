using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;

namespace EFCore.BulkExtensions;

/// <summary>
/// Class to initialize types using reflection
/// </summary>
public class FastProperty
{
    private static readonly ConcurrentDictionary<PropertyInfo, FastProperty> FastPropertyCache = new();

    /// <summary>
    /// Get or create a <see cref="FastProperty"/> instance for getting/setting the given property.
    /// </summary>
    /// <param name="property">The property to obtain a <see cref="FastProperty"/> instance for.</param>
    /// <returns>
    /// A new or already existing and cached <see cref="FastProperty"/> instance.
    /// </returns>
    public static FastProperty GetOrCreate(PropertyInfo property)
    {
        return FastPropertyCache.GetOrAdd(property, static p => new FastProperty(p));
    }

    /// <summary>
    /// Get or create a <see cref="FastProperty"/> instance for the given EF Core property metadata.
    /// </summary>
    public static FastProperty GetOrCreate(IProperty property, DbContext? dbContext = null)
    {
        if (property.PropertyInfo is not null)
        {
            return GetOrCreate(property.PropertyInfo);
        }

        if (dbContext is null)
        {
            throw new ArgumentNullException(nameof(dbContext), "A DbContext is required when creating a fast accessor for a shadow property.");
        }

        return new FastProperty(property, dbContext);
    }

    private Func<object, object?>? _getDelegate;
    private Action<object, object?>? _setDelegate;

    /// <summary>
    /// Constructor for FastPropery
    /// </summary>
    /// <param name="property"></param>
    private FastProperty(PropertyInfo property)
    {
        Property = property;
        InitializeGet();
        InitializeSet();
    }

    private FastProperty(IProperty property, DbContext dbContext)
    {
        MetadataProperty = property;
        Property = property.PropertyInfo;
        InitializeShadowGet(dbContext);
        InitializeShadowSet(dbContext);
    }

    private void InitializeSet()
    {
        var instance = Expression.Parameter(typeof(object), "instance");
        var value = Expression.Parameter(typeof(object), "value");

        if (Property is null || Property.DeclaringType is null)
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
            _setDelegate = Expression.Lambda<Action<object, object?>>(Expression.Call(instanceCast, setter, valueCast), new ParameterExpression[] { instance, value }).Compile();
    }

    private void InitializeGet()
    {
        var instance = Expression.Parameter(typeof(object), "instance");

        if (Property is null || Property.DeclaringType is null)
        {
            throw new ArgumentException("Unable to determine DeclaringType from Property");
        }

        UnaryExpression instanceCast = (!Property.DeclaringType.IsValueType)
            ? Expression.TypeAs(instance, Property.DeclaringType)
            : Expression.Convert(instance, Property.DeclaringType);

        var getter = Property.GetGetMethod(true) ?? Property.DeclaringType.GetProperty(Property.Name)?.GetGetMethod(true);

        if (getter != null)
            _getDelegate = Expression.Lambda<Func<object, object?>>(Expression.TypeAs(Expression.Call(instanceCast, getter), typeof(object)), instance).Compile();
    }

    private void InitializeShadowGet(DbContext dbContext)
    {
        _getDelegate = instance => dbContext.Entry(instance).Property(MetadataProperty!.Name).CurrentValue;
    }

    private void InitializeShadowSet(DbContext dbContext)
    {
        _setDelegate = (instance, value) => dbContext.Entry(instance).Property(MetadataProperty!.Name).CurrentValue = value;
    }

#pragma warning disable CS1591 // No XML comment required here
    public PropertyInfo? Property { get; set; }
    public IProperty? MetadataProperty { get; set; }
    public Type? UnderlyingType => Property?.PropertyType ?? MetadataProperty?.ClrType;

    /// <summary>
    /// Returns the object
    /// </summary>
    /// <param name="instance"></param>
    /// <returns></returns>
    public object? Get(object instance) => instance == default || _getDelegate is null ? default : _getDelegate(instance);

    /// <summary>
    /// Sets the delegate
    /// </summary>
    /// <param name="instance"></param>
    /// <param name="value"></param>
    public void Set(object instance, object? value)
    {
        if (instance == default || _setDelegate is null)
        {
            return;
        }

        _setDelegate(instance, value);
    }
}
