namespace EFCore.BulkExtensions.Tests.ShadowProperties;

public class SpModel
{
    public int Id { get; set; }

    public const string SpLong = nameof(SpLong);
    public const string SpNullableLong = nameof(SpNullableLong);
    public const string SpDateTime = nameof(SpDateTime);
}
