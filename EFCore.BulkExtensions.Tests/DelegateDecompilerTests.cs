using DelegateDecompiler.EntityFrameworkCore;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace EFCore.BulkExtensions.Tests;
public class DelegateDecompilerTests
{
    [Fact]
    public async Task DelegateDecompiler_DecompileAsync_WorksAsync()
    {
        using var context = new TestContext(ContextUtil.GetOptions());

        await context.Items
            .Where(x => x.ItemId < 0)
            .DecompileAsync()
            .BatchDeleteAsync()
        ;
    }
}
