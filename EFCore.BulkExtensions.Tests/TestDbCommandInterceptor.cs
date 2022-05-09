using Microsoft.EntityFrameworkCore.Diagnostics;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.Tests;

public class TestDbCommandInterceptor : DbCommandInterceptor
{
    /// <summary>
    /// Information about an intercepted <see cref="System.Data.Common.DbCommand"/>
    /// </summary>
    public record DbCommandInformation(IReadOnlyList<DbParameter> DbParameters, string Sql);

    public List<DbCommandInformation> AboutToBeExecutedCommands { get; } = new List<DbCommandInformation>();

    public List<DbCommandInformation> ExecutedNonQueryCommands { get; } = new List<DbCommandInformation>();
    public List<DbCommandInformation> ExecutedReaderCommands { get; } = new List<DbCommandInformation>();
    public List<DbCommandInformation> ExecutedScalarCommands { get; } = new List<DbCommandInformation>();

    protected static DbCommandInformation BuildCommandInformation(DbCommand DbCommand)
    {
        _ = DbCommand ?? throw new ArgumentNullException(nameof(DbCommand));

        var dbParameters = new List<DbParameter>();
        if (DbCommand.Parameters != null)
        {
            foreach (DbParameter parameter in DbCommand.Parameters)
                dbParameters.Add(parameter);
        }

        return new DbCommandInformation(dbParameters, DbCommand.CommandText);
    }

    public override int NonQueryExecuted(DbCommand command, CommandExecutedEventData eventData, int result)
    {
        if (command.CommandText != null)
        {
            lock (ExecutedNonQueryCommands)
            {
                ExecutedNonQueryCommands.Add(BuildCommandInformation(command));
            }
        }

        return base.NonQueryExecuted(command, eventData, result);
    }

    public override ValueTask<int> NonQueryExecutedAsync(DbCommand command, CommandExecutedEventData eventData, int result, CancellationToken cancellationToken = default)
    {
        if (command.CommandText != null)
        {
            lock (ExecutedNonQueryCommands)
            {
                ExecutedNonQueryCommands.Add(BuildCommandInformation(command));
            }
        }

        return base.NonQueryExecutedAsync(command, eventData, result, cancellationToken);
    }

    public override InterceptionResult<int> NonQueryExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
    {
        if (command.CommandText != null)
        {
            lock (AboutToBeExecutedCommands)
            {
                AboutToBeExecutedCommands.Add(BuildCommandInformation(command));
            }
        }

        return base.NonQueryExecuting(command, eventData, result);
    }

    public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
    {
        if (command.CommandText != null)
        {
            lock (AboutToBeExecutedCommands)
            {
                AboutToBeExecutedCommands.Add(BuildCommandInformation(command));
            }
        }

        return base.NonQueryExecutingAsync(command, eventData, result, cancellationToken);
    }

    public override DbDataReader ReaderExecuted(DbCommand command, CommandExecutedEventData eventData, DbDataReader result)
    {
        if (command.CommandText != null)
        {
            lock (ExecutedReaderCommands)
            {
                ExecutedReaderCommands.Add(BuildCommandInformation(command));
            }
        }

        return base.ReaderExecuted(command, eventData, result);
    }

    public override ValueTask<DbDataReader> ReaderExecutedAsync(DbCommand command, CommandExecutedEventData eventData, DbDataReader result, CancellationToken cancellationToken = default)
    {
        if (command.CommandText != null)
        {
            lock (ExecutedReaderCommands)
            {
                ExecutedReaderCommands.Add(BuildCommandInformation(command));
            }
        }

        return base.ReaderExecutedAsync(command, eventData, result, cancellationToken);
    }

    public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
    {
        if (command.CommandText != null)
        {
            lock (AboutToBeExecutedCommands)
            {
                AboutToBeExecutedCommands.Add(BuildCommandInformation(command));
            }
        }

        return base.ReaderExecuting(command, eventData, result);
    }

    public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
    {
        if (command.CommandText != null)
        {
            lock (AboutToBeExecutedCommands)
            {
                AboutToBeExecutedCommands.Add(BuildCommandInformation(command));
            }
        }

        return base.ReaderExecutingAsync(command, eventData, result, cancellationToken);
    }

    public override object? ScalarExecuted(DbCommand command, CommandExecutedEventData eventData, object? result)
    {
        if (command.CommandText != null)
        {
            lock (ExecutedScalarCommands)
            {
                ExecutedScalarCommands.Add(BuildCommandInformation(command));
            }
        }

        return base.ScalarExecuted(command, eventData, result);
    }

    public override ValueTask<object?> ScalarExecutedAsync(DbCommand command, CommandExecutedEventData eventData, object? result, CancellationToken cancellationToken = default)
    {
        if (command.CommandText != null)
        {
            lock (ExecutedScalarCommands)
            {
                ExecutedScalarCommands.Add(BuildCommandInformation(command));
            }
        }

        return base.ScalarExecutedAsync(command, eventData, result, cancellationToken);
    }

    public override InterceptionResult<object> ScalarExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<object> result)
    {
        if (command.CommandText != null)
        {
            lock (AboutToBeExecutedCommands)
            {
                AboutToBeExecutedCommands.Add(BuildCommandInformation(command));
            }
        }

        return base.ScalarExecuting(command, eventData, result);
    }

    public override ValueTask<InterceptionResult<object>> ScalarExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<object> result, CancellationToken cancellationToken = default)
    {
        if (command.CommandText != null)
        {
            lock (AboutToBeExecutedCommands)
            {
                AboutToBeExecutedCommands.Add(BuildCommandInformation(command));
            }
        }

        return base.ScalarExecutingAsync(command, eventData, result, cancellationToken);
    }
}
