namespace EntityFrameworkCore.Manipulation.Extensions
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using EntityFrameworkCore.Manipulation.Extensions.Configuration;
    using EntityFrameworkCore.Manipulation.Extensions.Configuration.Internal;
    using EntityFrameworkCore.Manipulation.Extensions.Internal;
    using EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Metadata;

    /// <summary>
    /// Extensions for <see cref="DbContext"/>.
    /// </summary>
    public static class SyncExtensions
    {
        public static async Task<ISyncResult<TEntity>> SyncAsync<TEntity>(this DbContext dbContext, IQueryable<TEntity> target, IReadOnlyCollection<TEntity> source, IClusivityBuilder<TEntity> insertClusivityBuilder = null, IClusivityBuilder<TEntity> updateClusivityBuilder = null, CancellationToken cancellationToken = default)
            where TEntity : class, new()
            => await SyncInternalAsync(
                dbContext ?? throw new ArgumentNullException(nameof(dbContext)),
                target ?? throw new ArgumentNullException(nameof(target)),
                source ?? throw new ArgumentNullException(nameof(source)),
                ignoreUpdates: false, // this is a full sync
                ignoreDeletions: false,
                insertClusivityBuilder,
                updateClusivityBuilder,
                cancellationToken);

        public static async Task<ISyncWithoutUpdateResult<TEntity>> SyncWithoutUpdateAsync<TEntity>(this DbContext dbContext, IQueryable<TEntity> target, IReadOnlyCollection<TEntity> source, IClusivityBuilder<TEntity> insertClusivityBuilder = null, CancellationToken cancellationToken = default)
            where TEntity : class, new()
            => await SyncInternalAsync(
                dbContext ?? throw new ArgumentNullException(nameof(dbContext)),
                target ?? throw new ArgumentNullException(nameof(target)),
                source ?? throw new ArgumentNullException(nameof(source)),
                ignoreUpdates: true, // this is a sync without updates
                ignoreDeletions: false,
                insertClusivityBuilder,
                null,
                cancellationToken);

        public static async Task<IUpsertResult<TEntity>> UpsertAsync<TEntity>(this DbContext dbContext, IReadOnlyCollection<TEntity> source, IClusivityBuilder<TEntity> insertClusivityBuilder = null, IClusivityBuilder<TEntity> updateClusivityBuilder = null, CancellationToken cancellationToken = default)
            where TEntity : class, new()
            => await SyncInternalAsync(
                dbContext ?? throw new ArgumentNullException(nameof(dbContext)),
                dbContext.Set<TEntity>(),
                source ?? throw new ArgumentNullException(nameof(source)),
                ignoreUpdates: false,
                ignoreDeletions: true, // this is a sync for upserts
                insertClusivityBuilder,
                updateClusivityBuilder,
                cancellationToken);

        private static async Task<SyncResult<TEntity>> SyncInternalAsync<TEntity>(
            this DbContext dbContext,
            IQueryable<TEntity> target,
            IReadOnlyCollection<TEntity> source,
            bool ignoreUpdates,
            bool ignoreDeletions,
            IClusivityBuilder<TEntity> insertClusivityBuilder,
            IClusivityBuilder<TEntity> updateClusivityBuilder,
            CancellationToken cancellationToken)
            where TEntity : class, new()
        {
            var stringBuilder = new StringBuilder();

            IEntityType entityType = dbContext.Model.FindEntityType(typeof(TEntity));

            IKey primaryKey = entityType.FindPrimaryKey();
            IProperty[] properties = entityType.GetProperties().ToArray();
            IProperty[] nonPrimaryKeyProperties = properties.Except(primaryKey.Properties).ToArray();

            IProperty[] propertiesToUpdate = updateClusivityBuilder == null ? nonPrimaryKeyProperties : updateClusivityBuilder.Build(nonPrimaryKeyProperties);
            IProperty[] propertiesToInsert = insertClusivityBuilder == null ? properties : primaryKey.Properties.Concat(insertClusivityBuilder.Build(nonPrimaryKeyProperties)).ToArray();

            (string targetCommand, IReadOnlyCollection<System.Data.SqlClient.SqlParameter> targetCommandParameters) = target.ToSqlCommand();

            var parameters = new List<object>(source.Count * properties.Length);
            parameters.AddRange(targetCommandParameters);

            bool isSqlite = dbContext.Database.IsSqlite();
            if (isSqlite)
            {
                stringBuilder.AddSqliteSyncCommand(
                    entityType: entityType,
                    source: source,
                    ignoreUpdates: ignoreUpdates,
                    ignoreDeletions: ignoreDeletions,
                    primaryKey: primaryKey,
                    nonPrimaryKeyProperties: nonPrimaryKeyProperties,
                    properties: properties,
                    propertiesToUpdate: propertiesToUpdate,
                    propertiesToInsert: propertiesToInsert,
                    parameters: parameters,
                    targetCommand: targetCommand);
            }
            else
            {
                await stringBuilder.AddSqlServerSyncCommand(
                    dbContext: dbContext,
                    entityType: entityType,
                    source: source,
                    ignoreUpdates: ignoreUpdates,
                    ignoreDeletions: ignoreDeletions,
                    primaryKey: primaryKey,
                    properties: properties,
                    nonPrimaryKeyProperties: nonPrimaryKeyProperties,
                    propertiesToUpdate: propertiesToUpdate,
                    propertiesToInsert: propertiesToInsert,
                    parameters: parameters,
                    targetCommand: targetCommand,
                    cancellationToken);
            }

            using Microsoft.EntityFrameworkCore.Storage.RelationalDataReader reader = await dbContext.Database.ExecuteSqlQueryAsync(stringBuilder.ToString(), parameters.ToArray(), cancellationToken);

            var insertedEntities = new List<TEntity>();
            var deletedEntities = new List<TEntity>();
            var updatedEntities = new List<(TEntity OldValue, TEntity NewValue)>();

            Func<object, object>[] propertyValueConverters = isSqlite ? EntityUtils.GetEntityPropertiesValueConverters(properties) : null;
            Func<object, object>[] keyValueConverters = isSqlite ? EntityUtils.GetEntityPropertiesValueConverters(primaryKey.Properties.ToArray()) : null;

            IProperty[] propertiesNotIncludedInUpdate = null;
            if (updateClusivityBuilder != null)
            {
                propertiesNotIncludedInUpdate = nonPrimaryKeyProperties.Except(propertiesToUpdate).ToArray();
            }

            IProperty[] propertiesNotIncludedInInsert = null;
            if (insertClusivityBuilder != null)
            {
                propertiesNotIncludedInInsert = properties.Except(propertiesToInsert).ToArray();
            }

            while (await reader.ReadAsync(cancellationToken))
            {
                object[] row = new object[1 + properties.Length]; // action + #properties
                reader.DbDataReader.GetValues(row);

                string action = row[0] as string;
                object[] keyPropertyValues = row.Skip(1).Take(primaryKey.Properties.Count).ToArray();

                if (string.Equals(action, "INSERT", StringComparison.OrdinalIgnoreCase))
                {
                    TEntity newValue = EntityUtils.FindEntityBasedOnKey(source, primaryKey, keyPropertyValues, keyValueConverters);
                    if (propertiesNotIncludedInInsert != null)
                    {
                        // If properties are included/excluded from the insert, then we have to set them to their default value to reflect the state in the DB
                        foreach (IProperty property in propertiesNotIncludedInInsert)
                        {
                            property.PropertyInfo.SetValue(newValue, null);
                        }
                    }

                    insertedEntities.Add(newValue);
                }
                else if (string.Equals(action, "DELETE", StringComparison.OrdinalIgnoreCase))
                {
                    deletedEntities.Add(EntityUtils.EntityFromRow<TEntity>(row, properties, 1, propertyValueConverters));
                }
                else
                {
                    // Update
                    TEntity oldValue = EntityUtils.EntityFromRow<TEntity>(row, properties, 1, propertyValueConverters);
                    TEntity newValue = EntityUtils.FindEntityBasedOnKey(source, primaryKey, keyPropertyValues, keyValueConverters);

                    // We have to bear in mind that properties might be included/excluded. In that case, we'll have to take these props' values from the oldValue
                    if (propertiesNotIncludedInUpdate != null)
                    {
                        foreach (IProperty property in propertiesNotIncludedInUpdate)
                        {
                            property.PropertyInfo.SetValue(newValue, property.PropertyInfo.GetValue(oldValue));
                        }
                    }

                    updatedEntities.Add((oldValue, newValue));
                }
            }

            return new SyncResult<TEntity>(deletedEntities, insertedEntities, updatedEntities);
        }

        private static void AddSqliteSyncCommand<TEntity>(
            this StringBuilder stringBuilder,
            IEntityType entityType,
            IReadOnlyCollection<TEntity> source,
            bool ignoreUpdates,
            bool ignoreDeletions,
            IKey primaryKey,
            IProperty[] properties,
            IProperty[] nonPrimaryKeyProperties,
            IProperty[] propertiesToUpdate,
            IProperty[] propertiesToInsert,
            List<object> parameters,
            string targetCommand)
            where TEntity : class, new()
        {
            string tableName = entityType.GetTableName();
            string SourceAliaser(string columnName) => $"source_{columnName}";
            string TargetAliaser(string columnName) => $"target_{columnName}";

            stringBuilder.AppendLine("BEGIN TRANSACTION;")
                         .AppendLine("DROP TABLE IF EXISTS EntityFrameworkManipulationSync;")
                         .AppendLine("CREATE TEMP TABLE EntityFrameworkManipulationSync AS ")
                         .AppendLine("WITH source AS ( ")
                            .AppendSelectFromInlineTable(properties, source, parameters, "x", sqliteSyntax: true)
                            .AppendLine("), ")
                         .AppendLine("target AS ( ")
                            .Append(targetCommand)
                            .AppendLine(") ")
                         .Append("SELECT (CASE WHEN (")
                            .AppendJoin(" AND ", primaryKey.Properties.Select(property => FormattableString.Invariant($"target.{property.Name} IS NULL")))
                            .Append(") THEN 'INSERT' ELSE 'UPDATE' END) AS _$action, ")
                            .AppendColumnNames(properties, false, "source", SourceAliaser).Append(", ")
                            .AppendColumnNames(properties, false, "target", TargetAliaser)
                            .Append("FROM source LEFT OUTER JOIN target ON ").AppendJoinCondition(primaryKey);

            // We ignore updates by not taking any matches in target (leaving us with only inserts, but crutially the target.* columns)
            if (ignoreUpdates)
            {
                stringBuilder.Append("WHERE _$action = 'INSERT'");
            }

            if (!ignoreDeletions)
            {
                stringBuilder
                    .AppendLine()
                    .AppendLine("UNION")
                    .Append("SELECT 'DELETE' AS _$action, ")
                        .AppendColumnNames(properties, false, "source", SourceAliaser).Append(", ")
                        .AppendColumnNames(properties, false, "target", TargetAliaser)
                        .Append("FROM target LEFT OUTER JOIN source ON ").AppendJoinCondition(primaryKey)
                        .Append("WHERE ").AppendJoin(" AND ", primaryKey.Properties.Select(property => FormattableString.Invariant($"source.{property.Name} IS NULL"))).AppendLine(";")
                    .Append("DELETE FROM ").Append(tableName).Append(" WHERE EXISTS (SELECT 1 FROM EntityFrameworkManipulationSync WHERE _$action='DELETE' AND ")
                        .AppendJoin(" AND ", primaryKey.Properties.Select(property => FormattableString.Invariant($"{property.Name}={TargetAliaser(property.Name)}"))).AppendLine(");");
            }
            else
            {
                stringBuilder.AppendLine(";");
            }

            // UPSERT
            stringBuilder
                .Append("INSERT OR REPLACE INTO ").Append(tableName).AppendColumnNames(propertiesToInsert, true)
                    .Append(" SELECT ").AppendJoin(",", propertiesToInsert.Select(m => SourceAliaser(m.GetColumnName())))
                    .AppendLine(" FROM EntityFrameworkManipulationSync WHERE _$action='INSERT' OR _$action='UPDATE' ");

            // There's no need to update if all rows are included in the primary key as nothing has changed.
            if (propertiesToUpdate.Any())
            {
                stringBuilder.Append("    ON CONFLICT ").AppendColumnNames(primaryKey.Properties, true).Append(" DO UPDATE SET ")
                        .AppendJoin(",", propertiesToUpdate.Select(property => FormattableString.Invariant($"{property.Name}=excluded.{property.Name}")));
            }
            stringBuilder.AppendLine(";");


            // Select the output
            stringBuilder.Append("SELECT ")
                    .AppendActionOutputColumns(primaryKey, nonPrimaryKeyProperties, SourceAliaser, TargetAliaser, "_$action")
                    .AppendLine(" FROM EntityFrameworkManipulationSync;")
                .Append("COMMIT;");
        }

        private static async Task AddSqlServerSyncCommand<TEntity>(
            this StringBuilder stringBuilder,
            DbContext dbContext,
            IEntityType entityType,
            IReadOnlyCollection<TEntity> source,
            bool ignoreUpdates,
            bool ignoreDeletions,
            IKey primaryKey,
            IProperty[] properties,
            IProperty[] nonPrimaryKeyProperties,
            IProperty[] propertiesToUpdate,
            IProperty[] propertiesToInsert,
            List<object> parameters,
            string targetCommand,
            CancellationToken cancellationToken)
            where TEntity : class, new()
        {
            ManipulationExtensionsConfiguration configuration = dbContext.GetConfiguration();

            if (configuration.SqlServerConfiguration.UseMerge)
            {
                bool outputInto = configuration.SqlServerConfiguration.EntityTypesWithTriggers.Contains(entityType.ClrType.Name);
                string userDefinedTableTypeName = null;
                if (configuration.SqlServerConfiguration.ShouldUseTableValuedParameters(properties, source) || outputInto)
                {
                    userDefinedTableTypeName = await dbContext.Database.CreateUserDefinedTableTypeIfNotExistsAsync(entityType, configuration.SqlServerConfiguration, cancellationToken);
                }

                if (outputInto)
                {
                    stringBuilder.AppendOutputDeclaration(userDefinedTableTypeName);
                }

                stringBuilder
                    .AppendLine("WITH TargetData AS (").Append(targetCommand).AppendLine(")")
                    .AppendLine("MERGE INTO TargetData AS target ")
                    .Append("USING ");

                if (configuration.SqlServerConfiguration.ShouldUseTableValuedParameters(properties, source))
                {
                    stringBuilder.AppendTableValuedParameter(userDefinedTableTypeName, properties, source, parameters);
                }
                else
                {
                    stringBuilder.Append("(").AppendSelectFromInlineTable(properties, source, parameters, "x").Append(")");
                }

                stringBuilder.Append(" AS source ON ").AppendJoinCondition(primaryKey).AppendLine(" ")
                    .Append("WHEN NOT MATCHED BY TARGET THEN INSERT ")
                    .AppendColumnNames(propertiesToInsert, wrapInParanthesis: true).Append("VALUES ")
                    .AppendColumnNames(propertiesToInsert, wrapInParanthesis: true, identifierPrefix: "source")
                    .AppendLine();

                if (!ignoreUpdates)
                {
                    stringBuilder.Append("WHEN MATCHED THEN UPDATE SET ").AppendJoin(",", propertiesToUpdate.Select(property => FormattableString.Invariant($"{property.Name}=source.{property.Name}"))).AppendLine();
                }

                if (!ignoreDeletions)
                {
                    stringBuilder.AppendLine("WHEN NOT MATCHED BY SOURCE THEN DELETE");
                }

                stringBuilder
                    .Append("OUTPUT ")
                    .AppendActionOutputColumns(primaryKey, nonPrimaryKeyProperties, column => $"inserted.{column}", column => $"deleted.{column}", "$action")
                    .Append(";");

                if (outputInto)
                {
                    stringBuilder.AppendOutputSelect(properties, includeAction: true).AppendLine(";");
                }
            }
            else
            {
                string userDefinedTableTypeName = null;
                userDefinedTableTypeName = await dbContext.Database.CreateUserDefinedTableTypeIfNotExistsAsync(entityType, configuration.SqlServerConfiguration, cancellationToken);
                string tvpParameter = SqlCommandBuilderExtensions.CreateTableValuedParameter(userDefinedTableTypeName, properties, source, parameters);

                string tableName = entityType.GetSchemaQualifiedTableName();
                stringBuilder
                    .AppendLine("SET NOCOUNT ON;")
                    .Append("DECLARE @InsertResult ").Append(userDefinedTableTypeName).AppendLine(";")
                    .AppendLine("BEGIN TRANSACTION;");

                // DELETE if not exist in source table
                if (!ignoreDeletions)
                {
                    stringBuilder
                        .Append("DECLARE @DeleteResult ").Append(userDefinedTableTypeName).AppendLine(";")
                        .AppendLine("WITH target AS (")
                            .Append(targetCommand).AppendLine(")")
                        .AppendLine("DELETE FROM target")
                        .Append("OUTPUT 'DELETE' AS __Action, ")
                            .AppendColumnNames(properties, wrapInParanthesis: false, "deleted")
                            .Append(" INTO ").AppendLine("@DeleteResult")
                        .Append("WHERE NOT EXISTS (SELECT 1 FROM ").Append(tvpParameter).Append(" source WHERE ").AppendJoinCondition(primaryKey).AppendLine(");");
                }

                // UPDATE
                if (!ignoreUpdates)
                {
                    stringBuilder
                        .Append("DECLARE @UpdateResult ").Append(userDefinedTableTypeName).AppendLine(";")
                        .Append("UPDATE ").Append(tableName).AppendLine(" SET")
                            .AppendJoin(",", propertiesToUpdate.Select(property => FormattableString.Invariant($"{property.Name}=source.{property.Name}"))).AppendLine()
                        .Append("OUTPUT 'UPDATE' AS __Action, ")
                            .AppendColumnNames(properties, wrapInParanthesis: false, "deleted")
                            .Append(" INTO ").AppendLine("@UpdateResult")
                        .Append("FROM ").Append(tableName).Append(" AS target INNER JOIN ").Append(tvpParameter).Append(" source ON ").AppendJoinCondition(primaryKey).AppendLine(";");
                }

                // INSERT if not exists in taget table
                string tableToCheckForExistence = ignoreUpdates ? tableName : "@UpdateResult";
                stringBuilder
                    .Append("INSERT INTO ").Append(tableName).AppendLine()
                    .Append("OUTPUT 'INSERT' AS __Action, ")
                        .AppendColumnNames(properties, wrapInParanthesis: false, "deleted")
                        .Append(" INTO ").AppendLine("@InsertResult")
                    .Append("SELECT ").AppendJoin(",", propertiesToInsert.Select(m => m.GetColumnName())).AppendLine()
                    .Append("FROM ").Append(tvpParameter).AppendLine(" source")
                    .Append("WHERE NOT EXISTS (SELECT 1 FROM ")
                        .Append(tableToCheckForExistence).Append(" WHERE ")
                        .AppendJoinCondition(primaryKey, rightTableAlias: tableName).AppendLine(");");

                // Return the sync result
                stringBuilder.Append("SELECT ").AppendColumnNames(properties, wrapInParanthesis: false).AppendLine(" FROM @InsertResult");

                if (!ignoreUpdates)
                {
                    stringBuilder
                        .AppendLine("UNION")
                        .Append("SELECT ").AppendColumnNames(properties, wrapInParanthesis: false).AppendLine(" FROM @UpdateResult");
                }

                if (!ignoreDeletions)
                {
                    stringBuilder
                        .AppendLine("UNION")
                        .Append("SELECT ").AppendColumnNames(properties, wrapInParanthesis: false).AppendLine(" FROM @DeleteResult");
                }

                stringBuilder
                    .AppendLine(";")
                    .Append("COMMIT TRANSACTION;");
            }
        }

        private static StringBuilder AppendActionOutputColumns(
            this StringBuilder stringBuilder,
            IKey primaryKey,
            IReadOnlyCollection<IProperty> nonPrimaryKeyProperties,
            Func<string, string> insertedAliaser,
            Func<string, string> deletedAliaser,
            string actionColumnName)
        {
            stringBuilder.Append(actionColumnName);

            foreach (IProperty keyProperty in primaryKey.Properties)
            {
                string columnName = keyProperty.GetColumnName();
                stringBuilder.Append("CASE WHEN ").Append(actionColumnName).Append(" = 'INSERT' THEN ")
                    .Append(insertedAliaser(columnName))
                    .Append(" ELSE deleted.").Append(deletedAliaser(columnName))
                    .Append(" END AS ").Append(columnName);
            }

            if (nonPrimaryKeyProperties.Count > 0)
            {
                stringBuilder
                    .Append(',')
                    .AppendJoin(',', nonPrimaryKeyProperties.Select(prop => deletedAliaser(prop.GetColumnName())));
            }

            return stringBuilder;
        }
    }
}
