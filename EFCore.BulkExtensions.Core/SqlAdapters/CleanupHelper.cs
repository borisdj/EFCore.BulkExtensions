using Microsoft.EntityFrameworkCore;
using System;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.SqlAdapters;

/// <summary>
/// Executes cleanup statements from a finally block: each one runs even if a previous one failed, with CancellationToken.None
/// (the caller token may be the reason of the failure). Errors never replace the original exception: they are ignored when the
/// operation failed (EF still logs them), the first one is thrown when the operation succeeded.
/// </summary>
internal static class CleanupHelper
{
    /// <summary>
    /// Executes each statement even if a previous one failed. The executor must not use the caller CancellationToken since it may be the reason the operation failed.
    /// The first error is thrown only when the operation succeeded, so it never hides the original exception.
    /// </summary>
    /// <param name="operationFailed">True when the operation threw, cleanup errors are then ignored</param>
    /// <param name="execute">Executes one statement</param>
    /// <param name="sqls">Statements to execute, null ones are skipped</param>
    public static async Task ExecuteAsync(bool operationFailed, Func<string, Task> execute, params string?[] sqls)
    {
        Exception? firstError = null;
        foreach (var sql in sqls)
        {
            if (sql == null)
            {
                continue;
            }
            try
            {
                await execute(sql).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                firstError ??= ex;
            }
        }
        if (firstError != null && !operationFailed)
        {
            ExceptionDispatchInfo.Throw(firstError);
        }
    }

    /// <summary>
    /// Same as <see cref="ExecuteAsync(bool, Func{string, Task}, string?[])"/>, statements are executed with ExecuteSqlRaw and CancellationToken.None
    /// </summary>
    /// <param name="dbContext"></param>
    /// <param name="isAsync"></param>
    /// <param name="operationFailed">True when the operation threw, cleanup errors are then ignored</param>
    /// <param name="sqls">Statements to execute, null ones are skipped</param>
    public static Task ExecuteAsync(DbContext dbContext, bool isAsync, bool operationFailed, params string?[] sqls)
    {
        return ExecuteAsync(operationFailed, sql => isAsync ? dbContext.Database.ExecuteSqlRawAsync(sql, CancellationToken.None)
                                                            : Task.FromResult(dbContext.Database.ExecuteSqlRaw(sql)), sqls);
    }
}
