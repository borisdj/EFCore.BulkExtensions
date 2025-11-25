using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Metadata;

using System;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions;

internal class GraphUtil
{
    public static IEnumerable<GraphNode>? GetSortedGraph(DbContext dbContext, IEnumerable<object> entities)
    {
        if (!entities.Any())
        {
            return null;
        }

        // Enumerate through all the entities and retrieve a flat list of all the entities with their dependencies
        var dependencies = new Dictionary<object, GraphDependency>();

        foreach (var e in entities)
        {
            GetFlatGraph(dbContext, e, dependencies);
        }

        // Sort these entities so the first entity is the least dependendant
        var topologicalSorted = TopologicalSort(dependencies.Keys, y => dependencies[y].DependsOn.Select(y => y.entity));

        var withAdditionalSorting = true;

        if (withAdditionalSorting) return GetNodesWithSortingByDepth(dependencies, topologicalSorted);

        return GetNodes(dependencies, topologicalSorted);
    }

    private static IEnumerable<GraphNode> GetNodes(Dictionary<object, GraphDependency> dependencies, IEnumerable<object> topologicalSorted)
    {
        var result = new List<GraphNode>();

        foreach (var s in topologicalSorted)
        {
            result.Add(new GraphNode
            {
                Entity = s,
                Dependencies = dependencies[s]
            });
        }

        return result;
    }

    private static IEnumerable<GraphNode> GetNodesWithSortingByDepth(Dictionary<object, GraphDependency> dependencies, IEnumerable<object> topologicalSorted)
    {
        var entitiesDepth = CalculateEntitiesDepth(dependencies, topologicalSorted.ToList());

        var typesDepth = CalculateTypesDepth(entitiesDepth);

        var result = new List<GraphNode>();

        foreach (var type in typesDepth.OrderByDescending(x => x.Value))
        {
            foreach (var s in topologicalSorted)
            {
                if (s.GetType() != type.Key) continue;

                result.Add(new GraphNode
                {
                    Entity = s,
                    Dependencies = dependencies[s]
                });
            }
        }

        return result;
    }

    /// <summary>
    /// Calculates the depth of each entity from the entity to the top of the dependency graph. Returns a dictionary mapping entities to their depth.
    /// </summary>
    private static Dictionary<object, int> CalculateEntitiesDepth(
        Dictionary<object, GraphDependency> graph,
        IReadOnlyList<object> topologicalSorted)
    {
        var depthDict = graph.Keys.ToDictionary(k => k, v => 0);

        for (int i = graph.Count - 1; i >= 0; i--)
        {
            var node = topologicalSorted[i];

            var nodeDeps = graph
                .Where(x => x.Value.DependsOn.Any(d => d.entity == node))
                .Select(x => x.Key)
                .ToList();

            int max = 0;
            foreach (var childNode in nodeDeps)
                max = Math.Max(max, depthDict[childNode] + 1);

            depthDict[node] = max;
        }

        return depthDict;
    }

    /// <summary>
    /// Calculates the maximum depth from entity of a given type to the top. Returns a dictionary mapping types to their maximum depth.
    /// </summary>
    private static Dictionary<Type, int> CalculateTypesDepth(Dictionary<object, int> entitiesDepth)
    {
        var typesDepth = new Dictionary<Type, int>();

        foreach (var entityDepth in entitiesDepth)
        {
            var type = entityDepth.Key.GetType();

            if (typesDepth.TryGetValue(type, out int value))
                typesDepth[type] = Math.Max(value, entityDepth.Value);
            else
                typesDepth.Add(type, entityDepth.Value);
        }

        return typesDepth;
    }

    private static GraphDependency? GetFlatGraph(DbContext dbContext, object graphEntity, IDictionary<object, GraphDependency> result)
    {
        var entityType = dbContext.Model.FindEntityType(graphEntity.GetType());

        // The entity is not being apart of the DbContext model, do nothing
        if (entityType is null)
            return null;

        if (!result.TryGetValue(graphEntity, out GraphDependency? graphDependency))
        {
            graphDependency = new GraphDependency();
            result.Add(graphEntity, graphDependency);
        }
        else
        {
            // To prevent circular references & stack overflow, if the graphEntity has already been tracked then just return
            return graphDependency;
        }

        var entityNavigations = entityType.GetNavigations();

        foreach (var navigation in entityNavigations)
        {
            if (navigation.IsCollection)
            {
                var navigationValue = dbContext.Entry(graphEntity).Collection(navigation.Name).CurrentValue;

                if (navigationValue is null)
                    continue;

                var navigationCollectionValue = navigationValue.Cast<object>().ToList();

                foreach (var navEntity in navigationCollectionValue)
                {
                    SetDependencies(dbContext, graphDependency, graphEntity, navigation, navEntity, result);
                }
            }
            else
            {
                var navigationValue = dbContext.Entry(graphEntity).Reference(navigation.Name).CurrentValue;

                if (navigationValue is null)
                    continue;

                SetDependencies(dbContext, graphDependency, graphEntity, navigation, navigationValue, result);
            }
        }

        return graphDependency;
    }

    private static void SetDependencies(DbContext dbContext, GraphDependency graphDependency, object graphEntity, INavigation navigation, object navigationValue, IDictionary<object, GraphDependency> result)
    {
        // Get the nested dependency for the navigationValue so we can add the inverse navigation dependency
        // incase the navigationValue entity does not have an explicitly defined navigation property back to its principal / dependent
        // i.e WorkOrderSpare.Spare but the Spare entity does not have a Spare.WorkOrderSpares navigation property
        var nestedDependency = GetFlatGraph(dbContext, navigationValue, result);

        if (nestedDependency is null)
            return;

        if (navigation.IsOnDependent

            // A navigation for an OwnedType will be dependent on its owner the in efcore dependency hierarchy,
            // but technically the Owner depends on the OwnedType if the OwnedType is part of its Owner's schema.
            || OwnedTypeUtil.IsOwnedInSameTableAsOwner(navigation))
        {
            graphDependency.DependsOn.Add((navigationValue, navigation));
            nestedDependency.Dependents.Add((graphEntity, navigation.Inverse ?? navigation));
        }
        else
        {
            graphDependency.Dependents.Add((navigationValue, navigation));
            nestedDependency.DependsOn.Add((graphEntity, navigation.Inverse ?? navigation));
        }
    }

    private static IEnumerable<T> TopologicalSort<T>(IEnumerable<T> source, Func<T, IEnumerable<T>> dependencies, bool throwOnCycle = false)
    {
        var sorted = new List<T>();
        var visited = new HashSet<T>();

        foreach (var item in source)
            Visit(item, visited, sorted, dependencies, throwOnCycle);

        return sorted;
    }

    private static void Visit<T>(T item, HashSet<T> visited, List<T> sorted, Func<T, IEnumerable<T>> dependencies, bool throwOnCycle)
    {
        if (!visited.Contains(item))
        {
            visited.Add(item);

            foreach (var dep in dependencies(item))
                Visit(dep, visited, sorted, dependencies, throwOnCycle);

            sorted.Add(item);
        }
        else
        {
            if (throwOnCycle && !sorted.Contains(item))
                throw new Exception("Cyclic dependency found");
        }
    }

    public class GraphNode
    {
        public object Entity { get; set; } = null!;
        public GraphDependency Dependencies { get; set; } = null!;
    }

    public class GraphDependency
    {
        public HashSet<(object entity, INavigation navigation)> DependsOn { get; } = new HashSet<(object, INavigation)>();
        public HashSet<(object entity, INavigation navigation)> Dependents { get; } = new HashSet<(object, INavigation)>();
    }
}
