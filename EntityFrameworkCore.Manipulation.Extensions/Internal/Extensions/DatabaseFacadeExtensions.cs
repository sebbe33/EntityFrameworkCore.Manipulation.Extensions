namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
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

    public static class DatabaseFacadeExtensions
    {
        private static readonly ConcurrentDictionary<(string DatabaseName, string EntityTableName), string> userDefinedTableTypeCache
            = new ConcurrentDictionary<(string, string), string>();

        public static Task<RelationalDataReader> ExecuteSqlQueryAsync(this DatabaseFacade databaseFacade, string sql, object[] parameters, CancellationToken cancellationToken)
        {
            IConcurrencyDetector concurrencyDetector = databaseFacade.GetService<IConcurrencyDetector>();

            using (concurrencyDetector.EnterCriticalSection())
            {
                RawSqlCommand rawSqlCommand = databaseFacade
                    .GetService<IRawSqlCommandBuilder>()
                    .Build(sql, parameters);

                IDiagnosticsLogger<DbLoggerCategory.Database.Command> diagnosticsLogger = databaseFacade.GetService<IDiagnosticsLogger<DbLoggerCategory.Database.Command>>();

                var paramObject = new RelationalCommandParameterObject(databaseFacade.GetService<IRelationalConnection>(), rawSqlCommand.ParameterValues, null, null, diagnosticsLogger);

                return rawSqlCommand.RelationalCommand.ExecuteReaderAsync(paramObject, cancellationToken);
            }
        }

        public static async Task<string> CreateUserDefinedTableTypeIfNotExistsAsync(this DatabaseFacade databaseFacade, IEntityType entityType, CancellationToken cancellationToken)
        {
            var stringBuilder = new StringBuilder();
            string entityTableName = entityType.GetTableName();

            var connectionInfo = new SqlConnectionStringBuilder(databaseFacade.GetDbConnection().ConnectionString
                ?? throw new InvalidOperationException("No connection string was specified for the connection to the database."));
            string fullyQualifiedDatabaseName = connectionInfo.DataSource + connectionInfo.InitialCatalog;


            // Check if the type has already been successfully created in the current database. If so, we don't need to generate the command to create the type.
            if (userDefinedTableTypeCache.TryGetValue((fullyQualifiedDatabaseName, entityTableName), out string userDefinedTableTypeName))
            {
                return userDefinedTableTypeName;
            }

            if (!ManipulationExtensionsConfiguration.TvpInterceptors.TryGetValue(entityType.ClrType, out ITableValuedParameterInterceptor interceptor))
            {
                interceptor = DefaultTableValuedParameterInterceptor.Instance;
            }

            // If the type hasn't been created (at least not by the running instance), then we send an idempotent command to create it.
            var schemaBuilder = new StringBuilder();

            System.Collections.Generic.IEnumerable<IInterceptedProperty> entityProperties = interceptor.OnCreatingProperties(entityType.GetProperties());

            foreach (IInterceptedProperty property in entityProperties)
            {
                schemaBuilder.Append(property.ColumnName).Append(' ').Append(property.ColumnType).Append(',');
            }

            schemaBuilder.Length--; // remove the last ","

            string schema = schemaBuilder.ToString();
            string schemaHash = schema.GetDeterministicStringHash();
            userDefinedTableTypeName = $"{entityTableName}_{schemaHash}";

            string typeIdClause = $"TYPE_ID('{userDefinedTableTypeName}')";

            stringBuilder
                .AppendLine("BEGIN TRANSACTION;")
                .Append("IF ").Append(typeIdClause).AppendLine(" IS NULL")
                .Append("CREATE TYPE ").Append(userDefinedTableTypeName).AppendLine(" AS TABLE")
                .Append("( ").Append(schema).AppendLine(" );")
                .Append("COMMIT;");
            try
            {
                await databaseFacade.ExecuteSqlRawAsync(stringBuilder.ToString(), cancellationToken);
            }
            catch (SqlException e) when (e.Message?.Contains("already exists") == true)
            {
                // Check if the type already exists
                bool doesExist = false;
                using RelationalDataReader reader = await databaseFacade.ExecuteSqlQueryAsync($"SELECT TYPE_NAME({typeIdClause}) AS UserDefinedTypeId", new object[0], cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    if (!reader.DbDataReader.IsDBNull(0))
                    {
                        doesExist = true;
                    }
                }

                if (!doesExist)
                {
                    throw;
                }
            }


            // Cache that the type now exists
            userDefinedTableTypeCache.TryAdd((fullyQualifiedDatabaseName, entityTableName), userDefinedTableTypeName);
            return userDefinedTableTypeName;
        }
    }
}
