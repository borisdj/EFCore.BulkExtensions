using EFCore.BulkExtensions.SQLAdapters.SQLServer;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions;

internal static class DbContextBulkTransactionSaveChanges
{
    #region SaveChanges
    public static void SaveChanges(DbContext context, BulkConfig bulkConfig, Action<decimal> progress)
    {
        SaveChangesAsync(context, bulkConfig, progress, CancellationToken.None, isAsync: false).GetAwaiter().GetResult();
    }

    public static async Task SaveChangesAsync(DbContext context, BulkConfig bulkConfig, Action<decimal> progress, CancellationToken cancellationToken)
    {
        await SaveChangesAsync(context, bulkConfig, progress, cancellationToken, isAsync: true).ConfigureAwait(false);
    }

    private static async Task SaveChangesAsync(DbContext context, BulkConfig bulkConfig, Action<decimal> progress, CancellationToken cancellationToken, bool isAsync)
    {
        // 2 ways:
        // OPTION 1) iteration with Dic and Fast member
        // OPTION 2) using Node model (here setting FK still not implemented)
        int option = 1;

        if (bulkConfig == null)
        {
            bulkConfig = new BulkConfig { };
        }
        if (bulkConfig.OnSaveChangesSetFK && bulkConfig.SetOutputIdentity == false) // When FK is set by DB then SetOutput is required
        {
            bulkConfig.SetOutputIdentity = true;
        }

        var entries = context.ChangeTracker.Entries();
        var entriesGroupedByEntity = entries.GroupBy(a => new { EntityType = a.Entity.GetType(), a.State },
                                                     (entry, group) => new { entry.EntityType, EntityState = entry.State, Entities = group.Select(a => a.Entity).ToList() });
        var entriesGroupedChanged = entriesGroupedByEntity.Where(a => EntityStateBulkMethodDict.Keys.Contains(a.EntityState) & a.Entities.Count >= 0);
        var entriesGroupedChangedSorted = entriesGroupedChanged.OrderBy(a => a.EntityState.ToString() != EntityState.Modified.ToString()).ToList();
        if (entriesGroupedChangedSorted.Count == 0)
            return;

        if (isAsync)
        {
            await context.Database.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
        }
        else
        {
            context.Database.OpenConnection();
        }
        var connection = context.GetUnderlyingConnection(bulkConfig);

        bool doExplicitCommit = false;
        if (context.Database.CurrentTransaction == null)
        {
            doExplicitCommit = true;
        }

        try
        {

            var transaction = context.Database.CurrentTransaction ?? context.Database.BeginTransaction();

            if (option == 1)
            {
                Dictionary<string, Dictionary<string, FastProperty>> fastPropertyDicts = new Dictionary<string, Dictionary<string, FastProperty>>();
                foreach (var entryGroup in entriesGroupedChangedSorted)
                {
                    Type entityType = entryGroup.EntityType;
                    entityType = (entityType.Namespace == "Castle.Proxies") ? entityType.BaseType : entityType;
                    var entityModelType = context.Model.FindEntityType(entityType);

                    var entityPropertyDict = new Dictionary<string, FastProperty>();
                    if (!fastPropertyDicts.ContainsKey(entityType.Name))
                    {
                        var properties = entityModelType.GetProperties();
                        var navigationPropertiesInfo = entityModelType.GetNavigations().Select(x => x.PropertyInfo);

                        foreach (var property in properties)
                        {
                            if (property.PropertyInfo != null) // skip Shadow Property
                            {
                                entityPropertyDict.Add(property.Name, new FastProperty(property.PropertyInfo));
                            }
                        }
                        foreach (var navigationPropertyInfo in navigationPropertiesInfo)
                        {
                            if (navigationPropertyInfo != null)
                            {
                                entityPropertyDict.Add(navigationPropertyInfo.Name, new FastProperty(navigationPropertyInfo));
                            }
                        }
                        fastPropertyDicts.Add(entityType.Name, entityPropertyDict);
                    }
                    else
                    {
                        entityPropertyDict = fastPropertyDicts[entityType.Name];
                    }

                    if (bulkConfig.OnSaveChangesSetFK)
                    {
                        var navigations = entityModelType.GetNavigations().Where(x => !x.IsCollection && !x.TargetEntityType.IsOwned());
                        if (navigations.Count() > 0)
                        {
                            foreach (var navigation in navigations)
                            {
                                // when FK entity was not modified it will not be in Dict, but also FK is auto set so no need here
                                if (fastPropertyDicts.ContainsKey(navigation.ClrType.Name)) // otherwise set it:
                                {
                                    var parentPropertyDict = fastPropertyDicts[navigation.ClrType.Name];
                                    var fkName = navigation.ForeignKey.Properties.FirstOrDefault().Name;
                                    var pkName = navigation.ForeignKey.PrincipalKey.Properties.FirstOrDefault().Name;

                                    foreach (var entity in entryGroup.Entities)
                                    {
                                        var parentEntity = entityPropertyDict[navigation.Name].Get(entity);
                                        var pkValue = parentPropertyDict[pkName].Get(parentEntity);
                                        entityPropertyDict[fkName].Set(entity, pkValue);
                                    }
                                }
                            }
                        }
                    }

                    string methodName = EntityStateBulkMethodDict[entryGroup.EntityState].Key;
                    if (isAsync)
                    {
                        await InvokeBulkMethod(context, entryGroup.Entities, entityType, methodName, bulkConfig, progress, cancellationToken, isAsync: true).ConfigureAwait(false);
                    }
                    else
                    {
                        InvokeBulkMethod(context, entryGroup.Entities, entityType, methodName, bulkConfig, progress, cancellationToken, isAsync: false).GetAwaiter().GetResult();
                    }
                }
            }
            else if (option == 2)
            {
                List<BulkMethodEntries> bulkMethodEntriesList = GetBulkMethodEntries(entries);
                foreach (var bulkMethod in bulkMethodEntriesList)
                {
                    if (isAsync)
                    {
                        await InvokeBulkMethod(context, bulkMethod.Entries, bulkMethod.Type, bulkMethod.MethodName, bulkConfig, progress, cancellationToken, isAsync: true).ConfigureAwait(false);
                    }
                    else
                    {
                        InvokeBulkMethod(context, bulkMethod.Entries, bulkMethod.Type, bulkMethod.MethodName, bulkConfig, progress, cancellationToken, isAsync: false).GetAwaiter().GetResult();
                    }
                }
            }
            if (doExplicitCommit)
            {
                transaction.Commit();
                context.ChangeTracker.AcceptAllChanges();
            }
        }
        finally
        {
            if (doExplicitCommit)
            {

                if (isAsync)
                {
                    await context.Database.CloseConnectionAsync().ConfigureAwait(false);
                }
                else
                {
                    context.Database.CloseConnection();
                }
            }
        }
    }

    private static async Task InvokeBulkMethod(DbContext context, List<object> entities, Type entityType, string methodName, BulkConfig bulkConfig, Action<decimal> progress, CancellationToken cancellationToken, bool isAsync)
    {
        methodName += isAsync ? "Async" : "";
        MethodInfo bulkMethod = typeof(DbContextBulkExtensions).GetMethods().Where(a => a.Name == methodName).ToList().FirstOrDefault();
        bulkMethod = bulkMethod.MakeGenericMethod(typeof(object));
        var arguments = new List<object> { context, entities, bulkConfig, progress, entityType, cancellationToken };
        if (isAsync)
        {
            var methodArguments = arguments.ToArray();
            await (Task)bulkMethod.Invoke(null, methodArguments);
        }
        else
        {
            arguments.RemoveAt(arguments.Count - 1); // removes cancellationToken
            var methodArguments = arguments.ToArray();
            bulkMethod.Invoke(null, methodArguments);
        }
    }

    private static Dictionary<EntityState, KeyValuePair<string, int>> EntityStateBulkMethodDict => new Dictionary<EntityState, KeyValuePair<string, int>>
    {
        { EntityState.Deleted, new KeyValuePair<string, int>(nameof(DbContextBulkExtensions.BulkDelete), 1) },
        { EntityState.Modified, new KeyValuePair<string, int>(nameof(DbContextBulkExtensions.BulkUpdate), 2) },
        { EntityState.Added, new KeyValuePair<string, int>(nameof(DbContextBulkExtensions.BulkInsert), 3)},
    };
    #endregion

    private static List<BulkMethodEntries> GetBulkMethodEntries(IEnumerable<EntityEntry> entries)
    {
        EntityEntry[] entryList = entries.ToArray();
        var tree = new Dictionary<Type, DbNode>();

        for (int i = 0; i < entryList.Length; i++)
        {
            EntityEntry entry = entryList[i];

            Type type = GetNonProxyType(entry.Entity.GetType());

            if (!tree.TryGetValue(type, out DbNode node))
            {
                node = new DbNode() { Type = type };
                tree.TryAdd(type, node);
            }

            node.AddEntry(entry);

            var navigations = entry.Navigations.Where(a => a.IsLoaded);

            foreach (var n in navigations.Where(a => a.Metadata.IsCollection))
            {
                Type navType = GetNonProxyType(n.Metadata.ClrType.GenericTypeArguments.Single());
                if (!tree.TryGetValue(navType, out DbNode childNode))
                {
                    childNode = new DbNode() { Type = navType };

                    tree.TryAdd(navType, childNode);
                };

                if (!childNode.Parents.Any(a => a.Type == node.Type))
                {
                    childNode.Parents.Add(node);
                }

                if (!node.Children.Any(a => a.Type == navType))
                {
                    node.Children.Add(childNode);
                }
            }

            foreach (var n in navigations.Where(a => !a.Metadata.IsCollection))
            {
                Type navType = GetNonProxyType(n.Metadata.ClrType);
                if (!tree.TryGetValue(navType, out DbNode parentNode))
                {
                    parentNode = new DbNode() { Type = navType };
                    tree.TryAdd(navType, parentNode);
                };

                if (!parentNode.Children.Any(a => a.Type == node.Type))
                {
                    parentNode.Children.Add(node);
                }

                if (!node.Parents.Any(a => a.Type == parentNode.Type))
                {
                    node.Parents.Add(parentNode);
                }
            }
        }

        var rootNodes = tree.Where(a => a.Value.Parents.Count == 0);
        var handledTypes = new Dictionary<Type, bool>();
        var bulkMehodEntriesList = new List<BulkMethodEntries>();

        bool TryAddNode(DbNode node)
        {
            if (node.Parents.All(a => handledTypes.TryGetValue(a.Type, out bool exists)))
            {
                if (!handledTypes.TryGetValue(node.Type, out bool exists))
                {
                    handledTypes.Add(node.Type, true);

                    foreach (var me in node.MethodEntries)
                    {
                        if (me.Value != null && me.Value.Count > 0)
                        {
                            bulkMehodEntriesList.Add(new BulkMethodEntries()
                            {
                                Type = node.Type,
                                MethodName = me.Key,
                                Entries = me.Value,
                            });
                        }
                    }
                }

                foreach (var p in node.Children)
                {
                    TryAddNode(p);
                }

                return exists;
            }

            return false;
        }

        foreach (var r in rootNodes)
        {
            TryAddNode(r.Value);
        }

        return bulkMehodEntriesList;
    }

    private static Type GetNonProxyType(Type type)
    {
        return (type.Namespace == "Castle.Proxies") ? type.BaseType : type;
    }

    internal class BulkMethodEntries
    {
        public BulkMethodEntries()
        {
            Entries = new List<object>();
        }

        public string MethodName { get; set; }

        public Type Type { get; set; }

        public List<object> Entries { get; set; }
    }
    internal class DbNode
    {
        public DbNode()
        {
            Parents = new List<DbNode>();
            Children = new List<DbNode>();
            MethodEntries = new KeyValuePair<string, List<object>>[EntityStateBulkMethodDict.Count];
        }

        public Type Type { get; set; }

        public List<DbNode> Parents { get; set; }

        public List<DbNode> Children { get; set; }

        public KeyValuePair<string, List<object>>[] MethodEntries { get; private set; }

        public void AddEntry(EntityEntry entry)
        {
            if (EntityStateBulkMethodDict.TryGetValue(entry.State, out KeyValuePair<string, int> method))
            {
                var methodEntry = MethodEntries.FirstOrDefault(a => a.Key == method.Key);
                if (methodEntry.Key == null)
                {
                    methodEntry = new KeyValuePair<string, List<object>>(method.Key, new List<object>());
                    MethodEntries[method.Value - 1] = methodEntry;
                }

                methodEntry.Value.Add(entry.Entity);
            }
        }
    }
}
