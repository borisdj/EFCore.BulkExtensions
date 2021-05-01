using EFCore.BulkExtensions.SqlAdapters;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions
{
    internal static class DbContextBulkTransactionGraphUtil
    {
        public static void ExecuteWithGraph(DbContext context, IEnumerable<object> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress)
        {
            ExecuteWithGraphAsync(context, entities, operationType, bulkConfig, progress, CancellationToken.None, isAsync: false).GetAwaiter().GetResult();
        }

        public static async Task ExecuteWithGraphAsync(DbContext context, IEnumerable<object> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress, CancellationToken cancellationToken)
        {
            await ExecuteWithGraphAsync(context, entities, operationType, bulkConfig, progress, cancellationToken, isAsync: true);
        }

        private static async Task ExecuteWithGraphAsync(DbContext context, IEnumerable<object> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress, CancellationToken cancellationToken, bool isAsync)
        {
            if (operationType != OperationType.Insert
                       && operationType != OperationType.InsertOrUpdate
                       && operationType != OperationType.InsertOrUpdateDelete
                       && operationType != OperationType.Update)
                throw new InvalidBulkConfigException($"{nameof(BulkConfig)}.{nameof(BulkConfig.IncludeGraph)} only supports Insert or Update operations.");

            // Sqlite bulk merge adapter does not support multiple objects of the same type with a zero value primary key
            if (SqlAdaptersMapping.GetDatabaseType(context) == DbServer.Sqlite)
                throw new NotSupportedException("Sqlite is not currently supported due to its BulkInsert implementation.");

            // If this is set to false, won't be able to propogate new primary keys to the relationships
            bulkConfig.SetOutputIdentity = true;

            // If this is set to false, wont' be able to support some code first model types as EFCore uses shadow properties when a relationship's foreign keys arent explicitly defined
            bulkConfig.EnableShadowProperties = true;

            var graphNodes = GraphUtil.GetTopologicallySortedGraph(context, entities);

            if (graphNodes == null)
                return;

            // Inserting an entity graph must be done within a transaction otherwise the database could end up in a bad state
            var hasExistingTransaction = context.Database.CurrentTransaction != null;
            var transaction = context.Database.CurrentTransaction ?? (isAsync ? await context.Database.BeginTransactionAsync() : context.Database.BeginTransaction());

            try
            {
                // Group the graph nodes by entity type so we can merge them into the database in batches, in the correct order of dependency (topological order)
                var graphNodesGroupedByType = graphNodes.GroupBy(y => y.Entity.GetType());

                foreach (var graphNodeGroup in graphNodesGroupedByType)
                {
                    // It is possible the object graph contains duplicate entities (by primary key) but the entities are different object instances in memory.
                    // This an happen when deserializing a nested JSON tree for example. So filter out the duplicates.
                    var entitiesToAction = GetUniqueEntities(context, graphNodeGroup.Select(y => y.Entity)).ToList();
                    var entityClrType = graphNodeGroup.Key;
                    var tableInfo = TableInfo.CreateInstance(context, entityClrType, entitiesToAction, operationType, bulkConfig);

                    if (isAsync)
                    {
                        await SqlBulkOperation.MergeAsync(context, entityClrType, entitiesToAction, tableInfo, operationType, progress, cancellationToken: cancellationToken);
                    }
                    else
                    {
                        SqlBulkOperation.Merge(context, entityClrType, entitiesToAction, tableInfo, operationType, progress);
                    }

                    // Set the foreign keys for dependents so they may be inserted on the next loop
                    var dependentsOfSameType = SetForeignKeysForDependentsAndYieldSameTypeDependents(context, entityClrType, graphNodeGroup).ToList();

                    // If there are any dependents of the same type (parent child relationship), then save those dependent entities again to commit the fk values
                    if (dependentsOfSameType.Any())
                    {
                        var dependentTableInfo = TableInfo.CreateInstance(context, entityClrType, dependentsOfSameType, operationType, bulkConfig);

                        if (isAsync)
                        {
                            await SqlBulkOperation.MergeAsync(context, entityClrType, dependentsOfSameType, dependentTableInfo, operationType, progress, cancellationToken: cancellationToken);
                        }
                        else
                        {
                            SqlBulkOperation.Merge(context, entityClrType, dependentsOfSameType, dependentTableInfo, operationType, progress);
                        }
                    }
                }

                if (hasExistingTransaction == false)
                {
                    if (isAsync)
                    {
                        await transaction.CommitAsync();
                    }
                    else
                    {
                        transaction.Commit();
                    }
                }
            }
            finally
            {
                if (hasExistingTransaction == false)
                {
                    if (isAsync)
                    {
                        await transaction.DisposeAsync();
                    }
                    else
                    {
                        transaction.Dispose();
                    }
                }
            }
        }

        private static IEnumerable<object> SetForeignKeysForDependentsAndYieldSameTypeDependents(DbContext context, Type entityClrType, IEnumerable<GraphUtil.GraphNode> graphNodeGroup)
        {
            // Loop through the dependants and update their foreign keys with the PK values of the just inserted / merged entities
            foreach (var graphNode in graphNodeGroup)
            {
                var entity = graphNode.Entity;

                foreach (var d in graphNode.Dependencies.Dependents)
                {
                    SetForeignKeyForRelationship(context, d.navigation, d.entity, entity);

                    if (d.entity.GetType() == entityClrType)
                    {
                        yield return d.entity;
                    }
                }
            }
        }

        private static IEnumerable<object> GetUniqueEntities(DbContext context, IEnumerable<object> entities)
        {
            var entityType = context.Model.FindEntityType(entities.First().GetType());
            var pk = entityType.FindPrimaryKey();
            var processedPks = new HashSet<PrimaryKeyList>();

            foreach (var entity in entities)
            {
                var entry = context.Entry(entity);

                // If the entry has its key set, make sure its unique. It is possible for an entity to exist more than once in a graph.
                if (entry.IsKeySet)
                {
                    var primaryKeyComparer = new PrimaryKeyList();

                    foreach (var pkProp in pk.Properties)
                    {
                        primaryKeyComparer.Add(entry.Property(pkProp.Name).CurrentValue);
                    }

                    // If the processed pk already exists in the HashSet, its not unique.
                    if (processedPks.Add(primaryKeyComparer))
                        yield return entity;
                }
                else
                {
                    yield return entity;
                }
            }
        }

        private static void SetForeignKeyForRelationship(DbContext context, INavigation navigation, object dependent, object principal)
        {
            var principalKeyProperties = navigation.ForeignKey.PrincipalKey.Properties;
            var pkValues = new List<object>();

            foreach (var pk in principalKeyProperties)
            {
                var value = context.Entry(principal).Property(pk.Name).CurrentValue;
                pkValues.Add(value);
            }

            var dependantKeyProperties = navigation.ForeignKey.Properties;

            for (int i = 0; i < pkValues.Count; i++)
            {
                var dk = dependantKeyProperties[i];
                var pkVal = pkValues[i];

                context.Entry(dependent).Property(dk.Name).CurrentValue = pkVal;
            }
        }

        private class PrimaryKeyList : List<object>
        {
            public override bool Equals(object obj)
            {
                var objCast = obj as PrimaryKeyList;

                if (objCast is null)
                    return base.Equals(objCast);

                if (objCast.Count != this.Count)
                    return base.Equals(objCast);

                for (int i = 0; i < this.Count; i++)
                {
                    var a = this[i];
                    var b = objCast[i];

                    if (a.Equals(b) == false)
                        return false;
                }

                return true;
            }

            public override int GetHashCode()
            {
                int hash = 0xC0FFEE;

                foreach (var x in this)
                {
                    hash = hash * 31 + x.GetHashCode();
                }

                return hash;
            }
        }
    }
}
