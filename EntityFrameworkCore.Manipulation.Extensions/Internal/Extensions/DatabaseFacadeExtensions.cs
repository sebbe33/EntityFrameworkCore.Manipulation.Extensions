using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Concurrent;
using System.Data.SqlClient;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
    public static class DatabaseFacadeExtensions
    {
        private static readonly ConcurrentDictionary<(string DatabaseName, string EntityTableName), string> userDefinedTableTypeCache
            = new ConcurrentDictionary<(string, string), string>();

        public static Task<RelationalDataReader> ExecuteSqlQueryAsync(this DatabaseFacade databaseFacade, string sql, object[] parameters, CancellationToken cancellationToken)
        {
            var concurrencyDetector = databaseFacade.GetService<IConcurrencyDetector>();

            using (concurrencyDetector.EnterCriticalSection())
            {
                var rawSqlCommand = databaseFacade
                    .GetService<IRawSqlCommandBuilder>()
                    .Build(sql, parameters);

                var diagnosticsLogger = databaseFacade.GetService<IDiagnosticsLogger<DbLoggerCategory.Database.Command>>();

                var paramObject = new RelationalCommandParameterObject(databaseFacade.GetService<IRelationalConnection>(), rawSqlCommand.ParameterValues, null, null, diagnosticsLogger);

                return rawSqlCommand.RelationalCommand.ExecuteReaderAsync(paramObject, cancellationToken);
            }
        }

        public static async Task<string> CreateUserDefinedTableTypeIfNotExistsAsync(this DatabaseFacade databaseFacade, IEntityType entityType, CancellationToken cancellationToken)
        {
            var stringBuilder = new StringBuilder();
            var entityTableName = entityType.GetTableName();

            var connectionInfo = new SqlConnectionStringBuilder(databaseFacade.GetDbConnection().ConnectionString
                ?? throw new InvalidOperationException("No connection string was specified for the connection to the database."));
            var fullyQualifiedDatabaseName = connectionInfo.DataSource + connectionInfo.InitialCatalog;

            string userDefinedTableTypeName;

            // Check if the type has already been successfully created in the current database. If so, we don't need to generate the command to create the type.
            if (userDefinedTableTypeCache.TryGetValue((fullyQualifiedDatabaseName, entityTableName), out userDefinedTableTypeName))
            {
                return userDefinedTableTypeName;
            }

            // If the type hasn't been created (at least not by the running instance), then we send an idempotent command to create it.
            var schemaBuilder = new StringBuilder();

            foreach (IProperty property in entityType.GetProperties())
            {
                schemaBuilder.Append(property.GetColumnName()).Append(' ').Append(property.GetColumnType()).Append(',');
            }

            schemaBuilder.Length--; // remove the last ","

            var schema = schemaBuilder.ToString();
            var schemaHash = schema.GetDeterministicStringHash();
            userDefinedTableTypeName = $"{entityTableName}_{schemaHash}";

            stringBuilder
                .Append("IF TYPE_ID('").Append(userDefinedTableTypeName).AppendLine("') IS NULL")
                .Append("CREATE TYPE ").Append(userDefinedTableTypeName).AppendLine(" AS TABLE")
                .Append("( ").Append(schema).Append(" )");

            await databaseFacade.ExecuteSqlRawAsync(stringBuilder.ToString(), cancellationToken);

            // Cache that the type now exists
            userDefinedTableTypeCache.TryAdd((fullyQualifiedDatabaseName, entityTableName), userDefinedTableTypeName);
            return userDefinedTableTypeName;
        }
    }
}
