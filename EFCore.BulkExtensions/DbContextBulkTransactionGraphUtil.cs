using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Linq;

namespace EFCore.BulkExtensions.Sqlite;

internal static class DbContextBulkTransactionGraphUtil
{
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
        var entityType = context.Model.FindEntityType(entities.First().GetType()) ?? throw new ArgumentException($"Unable to determine EntityType from given type {entities.First().GetType().Name}");
        var pk = entityType.FindPrimaryKey();
        var processedPks = new HashSet<PrimaryKeyList>();

        foreach (var entity in entities)
        {
            var entry = context.Entry(entity);

            // If the entry has its key set, make sure its unique. It is possible for an entity to exist more than once in a graph.
            if (entry.IsKeySet)
            {
                var primaryKeyComparer = new PrimaryKeyList();
                if (pk is not null)
                {
                    foreach (var pkProp in pk.Properties)
                    {
                        primaryKeyComparer.Add(entry.Property(pkProp.Name).CurrentValue);
                    }

                    // If the processed pk already exists in the HashSet, its not unique.
                    if (processedPks.Add(primaryKeyComparer))
                        yield return entity;
                }

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
        var pkValues = new List<object?>();

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

    private class PrimaryKeyList : List<object?>
    {
        public override bool Equals(object? obj)
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

                if (a?.Equals(b) == false)
                    return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            int hash = 0xC0FFEE;

            foreach (var x in this)
            {
                hash = hash * 31 + (x?.GetHashCode() ?? 0);
            }

            return hash;
        }
    }
}
