using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System.Linq;

namespace EFCore.BulkExtensions;

/// <summary>
/// Owned entity utilities
/// </summary>
public static class OwnedTypeUtil
{
    /// <summary>
    /// Determines if entity is owned entity
    /// </summary>
    /// <param name="owned"></param>
    /// <returns></returns>
    public static bool IsOwnedInSameTableAsOwner(IEntityType owned)
    {
        var ownership = owned.FindOwnership();

        if (ownership is null)
            return false;

        var owner = ownership.PrincipalEntityType;
        var ownedTables = owned.GetTableMappings();

        foreach (var ot in ownedTables)
        {
            var isSharingTable = ot.Table.EntityTypeMappings.Any(y => y.EntityType == owner);

            if (isSharingTable == false)
                return false;
        }

        return true;
    }

    /// <summary>
    /// Determines if entity is owned entity
    /// </summary>
    /// <param name="navigation"></param>
    /// <returns></returns>
    public static bool IsOwnedInSameTableAsOwner(INavigation navigation)
    {
        return IsOwnedInSameTableAsOwner(navigation.TargetEntityType);
    }
}
