using System.Diagnostics;
using System.Globalization;

namespace EFCore.BulkExtensions;

public static class ActivitySources
{
    private static readonly ActivitySource ActivitySource = new ActivitySource("EFCore.BulkExtensions");

    public static Activity StartExecuteActivity(OperationType operationType, int entitiesCount)
    {
        var activity = ActivitySource.StartActivity("EFCore.BulkExtensions.BulkExecute");
        if (activity != null)
        {
            activity.AddTag("operationType", operationType.ToString("G"));
            activity.AddTag("entitiesCount", entitiesCount.ToString(CultureInfo.InvariantCulture));
        }

        return activity;
    }
}
