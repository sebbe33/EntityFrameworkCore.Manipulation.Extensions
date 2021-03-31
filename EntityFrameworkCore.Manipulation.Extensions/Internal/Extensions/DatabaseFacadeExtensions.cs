namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
    using EntityFrameworkCore.Manipulation.Extensions.Configuration;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Diagnostics;
    using Microsoft.EntityFrameworkCore.Infrastructure;
    using Microsoft.EntityFrameworkCore.Metadata;
    using Microsoft.EntityFrameworkCore.Storage;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public static class DatabaseFacadeExtensions
    {
        private const string TableTypeGeneratorVersion = "1"; // This should be rev'd when the creation code for table types has changed

        private static readonly ConcurrentDictionary<(string DatabaseName, string EntityTableName, string Configuration), string> UserDefinedTableTypeCache
            = new ConcurrentDictionary<(string, string, string), string>();

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

        public static async Task<string> CreateUserDefinedTableTypeIfNotExistsAsync(
            this DatabaseFacade databaseFacade,
            IEntityType entityType,
            SqlServerManipulationExtensionsConfiguration configuration,
            CancellationToken cancellationToken)
        {
            var stringBuilder = new StringBuilder();
            string entityTableName = entityType.GetTableName();

            int hashBucketCount = 0;
            if (configuration.UseMemoryOptimizedTableTypes && configuration.HashIndexBucketCountsByEntityType?.TryGetValue(entityType.ClrType.Name, out hashBucketCount) != true)
            {
                hashBucketCount = configuration.DefaultHashIndexBucketCount;
            }

            var connectionInfo = new SqlConnectionStringBuilder(databaseFacade.GetDbConnection().ConnectionString
                ?? throw new InvalidOperationException("No connection string was specified for the connection to the database."));
            string fullyQualifiedDatabaseName = connectionInfo.DataSource + connectionInfo.InitialCatalog;

            // The cache configuration is a per-entity entry that defines the configuration that was used to set up the table type.
            // The configuration may change durning run-time, and as such we must account for that a table type may not be available when it changes. 
            string cacheConfiguration = $"{configuration.UseMemoryOptimizedTableTypes}-{hashBucketCount}";

            // Check if the type, with current configuration, has already been successfully created in the current database. If so, we don't need to generate the command to create the type.
            if (UserDefinedTableTypeCache.TryGetValue((fullyQualifiedDatabaseName, entityTableName, cacheConfiguration), out string userDefinedTableTypeName))
            {
                return userDefinedTableTypeName;
            }

            if (!configuration.TvpInterceptors.TryGetValue(entityType.ClrType, out ITableValuedParameterInterceptor interceptor))
            {
                interceptor = DefaultTableValuedParameterInterceptor.Instance;
            }

            // If the type hasn't been created (at least not by the running instance), then we send an idempotent command to create it.
            var schemaBuilder = new StringBuilder();

            IEnumerable<IInterceptedProperty> entityProperties = interceptor.OnCreatingProperties(entityType.GetProperties());

            foreach (IInterceptedProperty property in entityProperties)
            {
                schemaBuilder.Append(property.ColumnName).Append(' ').Append(property.ColumnType).Append(',');
            }

            schemaBuilder.Length--; // remove the last ","

            string schema = schemaBuilder.ToString();
            string schemaHash = schema.GetDeterministicStringHash();

            userDefinedTableTypeName = $"{entityTableName}_v{TableTypeGeneratorVersion}_{schemaHash}";

            string typeIdClause = $"TYPE_ID('{userDefinedTableTypeName}')";

            bool shouldUseMemoryOptimizedTableTypes = configuration.UseMemoryOptimizedTableTypes;

            if (shouldUseMemoryOptimizedTableTypes)
            {
                // Pre-check: First we need to check that the DB supports OLTP. We do this by issuing a query and reading a DB property.
                using RelationalDataReader reader = await databaseFacade.ExecuteSqlQueryAsync("SELECT DatabasePropertyEx(DB_NAME(), 'IsXTPSupported')", new object[0], cancellationToken);

                shouldUseMemoryOptimizedTableTypes = false; // for safety, assume that the DB doesn't support OLTP.
                while (await reader.ReadAsync(cancellationToken))
                {
                    shouldUseMemoryOptimizedTableTypes = reader.DbDataReader.GetByte(0) == 1;
                }
            }

            if (shouldUseMemoryOptimizedTableTypes)
            {
                // The result of the pre-check came back positive. Proceed with the creation of the memory-optimized type.
                IKey primaryKey = entityType.FindPrimaryKey();

                if (primaryKey == null)
                {
                    throw new InvalidOperationException("Cannot create a memory-optimized table type for an entity without a primary key");
                }

                // Override the table name + Type ID to be a memory type
                userDefinedTableTypeName = $"{entityTableName}_v{TableTypeGeneratorVersion}m_{schemaHash}";
                typeIdClause = $"TYPE_ID('{userDefinedTableTypeName}')";

                stringBuilder
                    .Append("IF ").Append(typeIdClause).AppendLine(" IS NULL")
                        .Append("CREATE TYPE ").Append(userDefinedTableTypeName).AppendLine(" AS TABLE")
                        .AppendLine("(")
                            .Append(schema).AppendLine(",")
                            .Append("INDEX IX_").Append(userDefinedTableTypeName).Append(" HASH ").AppendColumnNames(primaryKey.Properties, true).AppendLine()
                            .Append("WITH (BUCKET_COUNT = ").Append(hashBucketCount).AppendLine(")")
                        .AppendLine(") WITH (MEMORY_OPTIMIZED = ON)");
            }
            else
            {
                stringBuilder
                    .Append("IF ").Append(typeIdClause).AppendLine(" IS NULL")
                        .Append("CREATE TYPE ").Append(userDefinedTableTypeName).AppendLine(" AS TABLE")
                        .Append("( ").Append(schema).AppendLine(" )");
            }

            try
            {
                await databaseFacade.ExecuteSqlRawAsync(stringBuilder.ToString(), cancellationToken);
            }
            catch (SqlException e) when (e.Message?.Contains("already exists") == true)
            {
                // Check if the type already exists
                bool doesExist = false;
                using RelationalDataReader reader = await databaseFacade.ExecuteSqlQueryAsync($"SELECT TYPE_NAME({typeIdClause}) END AS UserDefinedTypeId", new object[0], cancellationToken);

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
            UserDefinedTableTypeCache.TryAdd((fullyQualifiedDatabaseName, entityTableName, cacheConfiguration), userDefinedTableTypeName);
            return userDefinedTableTypeName;
        }
    }
}
