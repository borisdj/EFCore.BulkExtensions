using Microsoft.EntityFrameworkCore.Diagnostics;
using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace EFCore.BulkExtensions.Tests
{
    public class TestDbCommandInterceptor : DbCommandInterceptor
    {
        public List<string> AboutToBeExecutedCommands { get; } = new List<string>();

        public List<string> ExecutedNonQueryCommands { get; } = new List<string>();
        public List<string> ExecutedReaderCommands { get; } = new List<string>();
        public List<string> ExecutedScalarCommands { get; } = new List<string>();

        public override int NonQueryExecuted(DbCommand command, CommandExecutedEventData eventData, int result)
        {
            if (command?.CommandText != null)
            {
                ExecutedNonQueryCommands.Add(command.CommandText);
            }

            return base.NonQueryExecuted(command, eventData, result);
        }

        public override Task<int> NonQueryExecutedAsync(DbCommand command, CommandExecutedEventData eventData, int result, CancellationToken cancellationToken = default)
        {
            if (command?.CommandText != null)
            {
                ExecutedNonQueryCommands.Add(command.CommandText);
            }

            return base.NonQueryExecutedAsync(command, eventData, result, cancellationToken);
        }

        public override InterceptionResult<int> NonQueryExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<int> result)
        {
            if (command?.CommandText != null)
            {
                AboutToBeExecutedCommands.Add(command.CommandText);
            }

            return base.NonQueryExecuting(command, eventData, result);
        }

        public override Task<InterceptionResult<int>> NonQueryExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
        {
            if (command?.CommandText != null)
            {
                AboutToBeExecutedCommands.Add(command.CommandText);
            }

            return base.NonQueryExecutingAsync(command, eventData, result, cancellationToken);
        }

        public override DbDataReader ReaderExecuted(DbCommand command, CommandExecutedEventData eventData, DbDataReader result)
        {
            if (command?.CommandText != null)
            {
                ExecutedReaderCommands.Add(command.CommandText);
            }

            return base.ReaderExecuted(command, eventData, result);
        }

        public override Task<DbDataReader> ReaderExecutedAsync(DbCommand command, CommandExecutedEventData eventData, DbDataReader result, CancellationToken cancellationToken = default)
        {
            if (command?.CommandText != null)
            {
                ExecutedReaderCommands.Add(command.CommandText);
            }

            return base.ReaderExecutedAsync(command, eventData, result, cancellationToken);
        }

        public override InterceptionResult<DbDataReader> ReaderExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result)
        {
            if (command?.CommandText != null)
            {
                AboutToBeExecutedCommands.Add(command.CommandText);
            }

            return base.ReaderExecuting(command, eventData, result);
        }

        public override Task<InterceptionResult<DbDataReader>> ReaderExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
        {
            if (command?.CommandText != null)
            {
                AboutToBeExecutedCommands.Add(command.CommandText);
            }

            return base.ReaderExecutingAsync(command, eventData, result, cancellationToken);
        }

        public override object ScalarExecuted(DbCommand command, CommandExecutedEventData eventData, object result)
        {
            if (command?.CommandText != null)
            {
                ExecutedScalarCommands.Add(command.CommandText);
            }

            return base.ScalarExecuted(command, eventData, result);
        }

        public override Task<object> ScalarExecutedAsync(DbCommand command, CommandExecutedEventData eventData, object result, CancellationToken cancellationToken = default)
        {
            if (command?.CommandText != null)
            {
                ExecutedScalarCommands.Add(command.CommandText);
            }

            return base.ScalarExecutedAsync(command, eventData, result, cancellationToken);
        }

        public override InterceptionResult<object> ScalarExecuting(DbCommand command, CommandEventData eventData, InterceptionResult<object> result)
        {
            if (command?.CommandText != null)
            {
                AboutToBeExecutedCommands.Add(command.CommandText);
            }

            return base.ScalarExecuting(command, eventData, result);
        }

        public override Task<InterceptionResult<object>> ScalarExecutingAsync(DbCommand command, CommandEventData eventData, InterceptionResult<object> result, CancellationToken cancellationToken = default)
        {
            if (command?.CommandText != null)
            {
                AboutToBeExecutedCommands.Add(command.CommandText);
            }

            return base.ScalarExecutingAsync(command, eventData, result, cancellationToken);
        }
    }
}
