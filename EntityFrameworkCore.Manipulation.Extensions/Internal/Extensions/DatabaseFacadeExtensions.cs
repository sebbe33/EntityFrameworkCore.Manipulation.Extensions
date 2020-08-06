using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
    public static class DatabaseFacadeExtensions
    {
        private static readonly ConcurrentDictionary<string, string> userDefinedTableTypeCache = new ConcurrentDictionary<string, string>();

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
            string userDefinedTableTypeName;

            // Check if the type has already been successfully create. If so, we don't need to generate the command to create type type
            if (userDefinedTableTypeCache.TryGetValue(entityTableName, out userDefinedTableTypeName))
            {
                return userDefinedTableTypeName;
            }


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

            userDefinedTableTypeCache.TryAdd(entityTableName, userDefinedTableTypeName);
            return userDefinedTableTypeName;
        }
    }
}
