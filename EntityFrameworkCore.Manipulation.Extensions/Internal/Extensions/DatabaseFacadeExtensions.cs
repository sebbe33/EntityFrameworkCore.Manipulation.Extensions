namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
    using EntityFrameworkCore.Manipulation.Extensions.Configuration;
    using EntityFrameworkCore.Manipulation.Extensions.Configuration.Internal;
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
        internal const string TempOutputTableActionColumn = "__Action";

        private const string TableTypeGeneratorVersion = "3"; // This should be rev'd when the creation code for table types has changed

        private static readonly ConcurrentDictionary<(string DatabaseName, string EntityTableName, string Configuration), string> UserDefinedTableTypeCache
            = new ConcurrentDictionary<(string, string, string), string>();

        private static readonly SemaphoreSlim TvpCreationLock = new SemaphoreSlim(1, 1);

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
            CancellationToken cancellationToken,
            bool includeActionColumn = false)
        {
            var stringBuilder = new StringBuilder();
            string entityTableName = entityType.GetTableName();

            int hashBucketCount = configuration.GetHashIndexBucketCount(entityType.ClrType);
            bool shouldUseMemoryOptimizedTableTypes = configuration.ShouldUseMemoryOptimizedTableTypes(entityType.ClrType);
            SqlServerTableTypeIndex indexType = configuration.GetTableTypeIndex(entityType.ClrType);

            var connectionInfo = new SqlConnectionStringBuilder(databaseFacade.GetDbConnection().ConnectionString
                ?? throw new InvalidOperationException("No connection string was specified for the connection to the database."));
            string fullyQualifiedDatabaseName = connectionInfo.DataSource + connectionInfo.InitialCatalog;

            // The cache configuration is a per-entity entry that defines the configuration that was used to set up the table type.
            // The configuration may change durning run-time, and as such we must account for that a table type may not be available when it changes. 
            string cacheConfiguration = $"{shouldUseMemoryOptimizedTableTypes}-{hashBucketCount}-{includeActionColumn}-{indexType}";

            // Check if the type, with current configuration, has already been successfully created in the current database. If so, we don't need to generate the command to create the type.
            if (UserDefinedTableTypeCache.TryGetValue((fullyQualifiedDatabaseName, entityTableName, cacheConfiguration), out string userDefinedTableTypeName))
            {
                return userDefinedTableTypeName;
            }

            ITableValuedParameterInterceptor interceptor = configuration.GetTvpInterceptor(entityType.ClrType);

            // If the type hasn't been created (at least not by the running instance), then we send an idempotent command to create it.
            var schemaBuilder = new StringBuilder();

            IEnumerable<IInterceptedProperty> entityProperties = interceptor.OnCreatingProperties(entityType.GetProperties());

            foreach (IInterceptedProperty property in entityProperties)
            {
                schemaBuilder.Append(property.ColumnName).Append(' ').Append(property.ColumnType).Append(',');
            }

            // Check if we should append __Action for use as output variable if performing syncs.
            if (includeActionColumn)
            {
                schemaBuilder.Append(TempOutputTableActionColumn).Append(" CHAR(6)");
            }
            else
            {
                schemaBuilder.Length--; // Remove last ,
            }

            string schema = schemaBuilder.ToString();
            string schemaHash = schema.GetDeterministicStringHash();


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

            IKey primaryKey = entityType.FindPrimaryKey();
            string typeIdClause = null;

            if (shouldUseMemoryOptimizedTableTypes)
            {
                // Non-clustered and hash index are the only indices available for memory optimized tables
                if (indexType != SqlServerTableTypeIndex.NonClusteredIndex)
                {
                    indexType = SqlServerTableTypeIndex.HashIndex;
                }

                // The result of the pre-check came back positive. Proceed with the creation of the memory-optimized type.
                (userDefinedTableTypeName, typeIdClause) = GetTableTypeInfo(entityTableName, indexType, includeActionColumn, isMemoryOptimized: true, schemaHash);

                stringBuilder
                    .Append("IF ").Append(typeIdClause).AppendLine(" IS NULL")
                        .Append("CREATE TYPE ").Append(userDefinedTableTypeName).AppendLine(" AS TABLE")
                        .AppendLine("(")
                            .Append(schema).AppendLine(",");

                if (indexType == SqlServerTableTypeIndex.NonClusteredIndex)
                {
                    stringBuilder.Append("PRIMARY KEY NONCLUSTERED ").AppendColumnNames(primaryKey.Properties, true).AppendLine();
                }
                else
                {
                    stringBuilder.Append("PRIMARY KEY NONCLUSTERED HASH ").AppendColumnNames(primaryKey.Properties, true).AppendLine()
                                .Append("WITH (BUCKET_COUNT = ").Append(hashBucketCount).AppendLine(")");
                }

                stringBuilder.AppendLine(") WITH (MEMORY_OPTIMIZED = ON)");
            }
            else
            {
                // Hash index is not available for regular table types
                if (indexType == SqlServerTableTypeIndex.HashIndex || indexType == SqlServerTableTypeIndex.Default)
                {
                    indexType = SqlServerTableTypeIndex.NoIndex;
                }

                (userDefinedTableTypeName, typeIdClause) = GetTableTypeInfo(entityTableName, indexType, includeActionColumn, isMemoryOptimized: false, schemaHash);

                stringBuilder
                    .Append("IF ").Append(typeIdClause).AppendLine(" IS NULL")
                        .Append("CREATE TYPE ").Append(userDefinedTableTypeName).AppendLine(" AS TABLE")
                        .Append("( ")
                            .Append(schema);

                if (indexType == SqlServerTableTypeIndex.NonClusteredIndex || indexType == SqlServerTableTypeIndex.ClusteredIndex)
                {
                    stringBuilder.Append(", PRIMARY KEY ")
                        .Append(indexType == SqlServerTableTypeIndex.NonClusteredIndex ? "NONCLUSTERED" : "CLUSTERED")
                        .AppendColumnNames(primaryKey.Properties, true).AppendLine();
                }

                stringBuilder.AppendLine(" )");
            }

            try
            {
                // We'll take two concurrency precautions when creating the TVP.
                //   1. Lock down the creation of the type to one caller at a time, avoiding conflicts at a local level
                //   2. If another caller, not on a local level, creates the TVP before us, then we'll catch the exception and check that the type has been created.
                await TvpCreationLock.WaitAsync(cancellationToken);
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
            finally
            {
                TvpCreationLock.Release();
            }


            // Cache that the type now exists
            UserDefinedTableTypeCache.TryAdd((fullyQualifiedDatabaseName, entityTableName, cacheConfiguration), userDefinedTableTypeName);
            return userDefinedTableTypeName;
        }

        private static (string userDefinedTableTypeName, string typeIdClause) GetTableTypeInfo(string entityTableName, SqlServerTableTypeIndex indexType, bool includeActionColumn, bool isMemoryOptimized, string schemaHash)
        {
            string userDefinedTableTypeName = $"{entityTableName}_v{TableTypeGeneratorVersion}{(isMemoryOptimized ? "m" : string.Empty)}{(includeActionColumn ? "a" : string.Empty)}_{indexType:D}_{schemaHash}";

            return (userDefinedTableTypeName, $"TYPE_ID('{userDefinedTableTypeName}')");
        }
    }
}
