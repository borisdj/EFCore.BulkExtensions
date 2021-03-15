using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;

namespace EFCore.BulkExtensions
{
    internal class GraphUtil
    {
        public class GraphItem
        {
            public Type EntityClrType { get; set; }
            public IEntityType EntityType { get; set; }
            public IList<GraphEntity> Entities { get; set; } = new List<GraphEntity>();

            public INavigation ParentNavigation { get; set; }
            public GraphItem Parent { get; set; }
            public ICollection<GraphItem> Relationships { get; set; } = new List<GraphItem>();
        }

        public class GraphEntity
        {
            public object Entity { get; set; }
            public object ParentEntity { get; set; }

            public INavigation InboundNavigation { get; set; }
        }

        public static IEnumerable<GraphItem> GetOrderedGraph(DbContext dbContext, IEnumerable<object> entities)
        {
            if (!entities.Any())
            {
                return null;
            }

            var entityGraphMap = new Dictionary<object, GraphItem>();
            var result = new HashSet<GraphItem>();

            foreach (var e in entities)
            {
                BuildRelationshipGraph(dbContext, new GraphEntity { Entity = e }, entityGraphMap, result);
            }

            var ordered = result.OrderBy((x) =>
            {
                if (x.Parent is null)
                {
                    for (int i = 0; i < x.Relationships.Count; i++)
                    {
                        var r = x.Relationships.ElementAt(i);

                        // If x's side of the relationship is dependent on the other side of the relationship
                        if (r.ParentNavigation.IsDependentToPrincipal())
                            // Return true to push it further down the list
                            return true;
                    }

                    // If x is not dependent on any of its relationships, push it up the list
                    return false;
                }

                // If the other side of the relationship is dependent on x
                if (x.ParentNavigation.IsDependentToPrincipal())
                {
                    // return false to push it up the list
                    return false;
                }

                return true;
            }).ToList();

            return ordered;
        }

        private static void BuildRelationshipGraph(DbContext dbContext, GraphEntity graphEntity, IDictionary<object, GraphItem> entityGraphMap, ICollection<GraphItem> result)
        {
            var entryEntity = graphEntity.Entity;
            var entryEntityType = entryEntity.GetType();
            var entityType = dbContext.Model.FindEntityType(entryEntityType);
            var sourceEntryEntity = graphEntity.ParentEntity;

            GraphItem parentGraphItem;
            GraphItem graphItem;

            if (sourceEntryEntity != null)
            {
                parentGraphItem = entityGraphMap[sourceEntryEntity];
                graphItem = parentGraphItem.Relationships.FirstOrDefault(y => y.EntityClrType == entryEntityType);
            }
            else
            {
                parentGraphItem = null;
                graphItem = result.FirstOrDefault(y => y.EntityClrType == entryEntityType);
            }

            if (graphItem is null)
            {
                graphItem = new GraphItem
                {
                    EntityClrType = entryEntityType,
                    Entities =
                    {
                        graphEntity
                    },
                    Parent = parentGraphItem,
                    ParentNavigation = graphEntity.InboundNavigation
                };

                if (parentGraphItem != null)
                    parentGraphItem.Relationships.Add(graphItem);
            }
            else
            {
                graphItem.Entities.Add(graphEntity);
            }

            // Always track in case the entryEntity has dependents
            if (entityGraphMap.ContainsKey(entryEntity) == false)
                entityGraphMap.Add(entryEntity, graphItem);

            result.Add(graphItem);
            EnumerateRelationshipValues(dbContext, entryEntity, entityType, entityGraphMap, result);
        }

        private static void EnumerateRelationshipValues(DbContext dbContext, object entryEntity, IEntityType entityType, IDictionary<object, GraphItem> entityGraphMap, ICollection<GraphItem> result)
        {
            var entityNavigations = entityType.GetNavigations();

            foreach (var navigation in entityNavigations)
            {
                if (navigation.IsCollection())
                {
                    var navigationValue = dbContext.Entry(entryEntity).Collection(navigation.Name).CurrentValue;

                    if (navigationValue is null)
                        continue;

                    var navigationCollectionValue = navigationValue.Cast<object>().ToList();

                    foreach (var navEntity in navigationCollectionValue)
                    {
                        BuildRelationshipGraph(
                            dbContext: dbContext,
                            graphEntity: new GraphEntity
                            {
                                Entity = navEntity,
                                ParentEntity = entryEntity,
                                InboundNavigation = navigation
                            },
                            entityGraphMap: entityGraphMap,
                            result: result);
                    }
                }
                else
                {
                    var navigationValue = dbContext.Entry(entryEntity).Reference(navigation.Name).CurrentValue;

                    if (navigationValue is null)
                        continue;

                    BuildRelationshipGraph(
                        dbContext: dbContext,
                        graphEntity: new GraphEntity
                        {
                            Entity = navigationValue,
                            ParentEntity = entryEntity,
                            InboundNavigation = navigation
                        },
                        entityGraphMap: entityGraphMap,
                        result: result);
                }
            }
        }
    }
}
