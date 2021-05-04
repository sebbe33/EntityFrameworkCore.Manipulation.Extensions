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
        /// <summary>
        /// Syncs the <paramref name="source"/> entities into the <paramref name="target"/> queryable. This entails:
        /// 1. Inserting any entities which exist in source, but not in target, into target
        /// 2. Updating the properties of any entities which exist in both source and target to the values found in source
        /// 3. Deleting any entities in target which do not exist in targets.
        ///
        /// This operation performs a full sync (also known as MERGE), and is to be used in scenarios where a target should replicate
        /// the source. For situations that do not require deletion, use
        /// <see cref="UpsertAsync{TEntity}(DbContext, IReadOnlyCollection{TEntity}, IClusivityBuilder{TEntity}, IClusivityBuilder{TEntity}, CancellationToken)"/>
        /// and for scenarios which do not require updates, use <see cref="SyncAsync{TEntity}(DbContext, IQueryable{TEntity}, IReadOnlyCollection{TEntity}, IClusivityBuilder{TEntity}, IClusivityBuilder{TEntity}, CancellationToken)"/>.
        /// </summary>
        /// <remarks>
        /// The <paramref name="target"/> should be selected with care as any entities not matched in <paramref name="source"/> will be deleted from the target.
        /// If you're only interesting in syncing a subset, make sure to filter/scope down the <paramref name="target"/> to meet your needs before passing it in.
        /// If you want to sync an entire table, simply pass the <see cref="DbSet{TEntity}"/> for your entity type, but note that this will delete any entities not found in source.
        /// </remarks>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="dbContext">EF Db Context</param>
        /// <param name="target">The target to be synced to. Specify a queryable based on the <see cref="DbSet{TEntity}"/> of your entity type. Specify with care.</param>
        /// <param name="source">The collection of entities to sync from.</param>
        /// <param name="insertClusivityBuilder">The clusivity builder for entities which will be inserted.</param>
        /// <param name="updateClusivityBuilder">The clusivity builder for entities which will be updated. You may use this to only update a subset of properties.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The result of the sync, containing which entities were inserted, updated, and deleted.</returns>
        public static async Task<ISyncResult<TEntity>> SyncAsync<TEntity>(this DbContext dbContext, IQueryable<TEntity> target, IReadOnlyCollection<TEntity> source, IClusivityBuilder<TEntity> insertClusivityBuilder = null, IClusivityBuilder<TEntity> updateClusivityBuilder = null, CancellationToken cancellationToken = default)
            where TEntity : class, new()
            => await SyncInternalAsync(
                dbContext ?? throw new ArgumentNullException(nameof(dbContext)),
                target ?? throw new ArgumentNullException(nameof(target)),
                targetResolver: null,
                source ?? throw new ArgumentNullException(nameof(source)),
                ignoreUpdates: false, // this is a full sync
                ignoreDeletions: false,
                insertClusivityBuilder,
                updateClusivityBuilder,
                cancellationToken);

        /// <summary>
        /// Syncs the <paramref name="source"/> entities into the a target. This entails:
        /// 1. Inserting any entities which exist in source, but not in target, into target
        /// 2. Updating the properties of any entities which exist in both source and target to the values found in source
        /// 3. Deleting any entities in target which do not exist in targets.
        ///
        /// The target is the queryable returned by the <paramref name="targetResolver"/>. You may use the source queryable, passed into the resolver,
        /// to specify the target queryable to return.
        ///
        /// This operation performs a full sync (also known as MERGE), and is to be used in scenarios where a target should replicate
        /// the source. For situations that do not require deletion, use
        /// <see cref="UpsertAsync{TEntity}(DbContext, IReadOnlyCollection{TEntity}, IClusivityBuilder{TEntity}, IClusivityBuilder{TEntity}, CancellationToken)"/>
        /// and for scenarios which do not require updates, use <see cref="SyncAsync{TEntity}(DbContext, IQueryable{TEntity}, IReadOnlyCollection{TEntity}, IClusivityBuilder{TEntity}, IClusivityBuilder{TEntity}, CancellationToken)"/>.
        /// </summary>
        /// <remarks>
        /// The <paramref name="targetResolver"/> should be specified with care as any entities not matched in <paramref name="source"/> will be deleted from the target.
        /// If you're only interesting in syncing a subset, make sure to filter/scope down the tarfet to meet your needs before passing it in.
        /// If you want to sync an entire table, simply pass the <see cref="DbSet{TEntity}"/> for your entity type, but note that this will delete any entities not found in source.
        /// </remarks>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="dbContext">EF Db Context</param>
        /// <param name="targetResolver">
        /// The resolver for the target. The <see cref="IQueryable{T}"/> returned by the resolver is used as the target. You may base the target on the
        /// target passed as input, and the source passed as input (e.g. by joining).
        /// </param>
        /// <param name="source">The collection of entities to sync from.</param>
        /// <param name="insertClusivityBuilder">The clusivity builder for entities which will be inserted.</param>
        /// <param name="updateClusivityBuilder">The clusivity builder for entities which will be updated. You may use this to only update a subset of properties.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The result of the sync, containing which entities were inserted, updated, and deleted.</returns>
        public static async Task<ISyncResult<TEntity>> SyncAsync<TEntity>(
            this DbContext dbContext,
            Func<(IQueryable<TEntity> source, IQueryable<TEntity> target), IQueryable<TEntity>> targetResolver,
            IReadOnlyCollection<TEntity> source,
            IClusivityBuilder<TEntity> insertClusivityBuilder = null,
            IClusivityBuilder<TEntity> updateClusivityBuilder = null,
            CancellationToken cancellationToken = default)
            where TEntity : class, new()
            => await SyncInternalAsync(
                dbContext ?? throw new ArgumentNullException(nameof(dbContext)),
                target: null,
                targetResolver ?? throw new ArgumentNullException(nameof(targetResolver)),
                source ?? throw new ArgumentNullException(nameof(source)),
                ignoreUpdates: false, // this is a full sync
                ignoreDeletions: false,
                insertClusivityBuilder,
                updateClusivityBuilder,
                cancellationToken);

        /// <summary>
        /// Syncs the <paramref name="source"/> entities into the <paramref name="target"/> queryable, without updating matched entities. This entails:
        /// 1. Inserting any entities which exist in source, but not in target, into target
        /// 2. Deleting any entities in target which do not exist in targets.
        /// </summary>
        /// <remarks>
        /// The <paramref name="target"/> should be selected with care as any entities not matched in <paramref name="source"/> will be deleted from the target.
        /// If you're only interesting in syncing a subset, make sure to filter/scope down the <paramref name="target"/> to meet your needs before passing it in.
        /// If you want to sync an entire table, simply pass the <see cref="DbSet{TEntity}"/> for your entity type, but note that this will delete any entities not found in source.
        /// </remarks>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="dbContext">EF Db Context</param>
        /// <param name="target">The target to be synced to. Specify a queryable based on the <see cref="DbSet{TEntity}"/> of your entity type. Specify with care.</param>
        /// <param name="source">The collection of entities to sync from.</param>
        /// <param name="insertClusivityBuilder">The clusivity builder for entities which will be inserted.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The result of the sync, containing which entities were inserted and deleted.</returns>
        public static async Task<ISyncWithoutUpdateResult<TEntity>> SyncWithoutUpdateAsync<TEntity>(this DbContext dbContext, IQueryable<TEntity> target, IReadOnlyCollection<TEntity> source, IClusivityBuilder<TEntity> insertClusivityBuilder = null, CancellationToken cancellationToken = default)
            where TEntity : class, new()
            => await SyncInternalAsync(
                dbContext ?? throw new ArgumentNullException(nameof(dbContext)),
                target ?? throw new ArgumentNullException(nameof(target)),
                targetResolver: null,
                source ?? throw new ArgumentNullException(nameof(source)),
                ignoreUpdates: true, // this is a sync without updates
                ignoreDeletions: false,
                insertClusivityBuilder,
                null,
                cancellationToken);

        /// <summary>
        /// Syncs the <paramref name="source"/> entities into a target, without updating matched entities. This entails:
        /// 1. Inserting any entities which exist in source, but not in target, into target
        /// 2. Deleting any entities in target which do not exist in targets.
        ///
        /// The target is the queryable returned by the <paramref name="targetResolver"/>. You may use the source queryable, passed into the resolver,
        /// to specify the target queryable to return.
        /// </summary>
        /// <remarks>
        /// The <paramref name="targetResolver"/> should be specified with care as any entities not matched in <paramref name="source"/> will be deleted from the target.
        /// If you're only interesting in syncing a subset, make sure to filter/scope down the tarfet to meet your needs before passing it in.
        /// If you want to sync an entire table, simply pass the <see cref="DbSet{TEntity}"/> for your entity type, but note that this will delete any entities not found in source.
        /// </remarks>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="dbContext">EF Db Context</param>
        /// <param name="targetResolver">
        /// The resolver for the target. The <see cref="IQueryable{T}"/> returned by the resolver is used as the target. You may base the target on the
        /// target passed as input, and the source passed as input (e.g. by joining).
        /// </param>
        /// <param name="source">The collection of entities to sync from.</param>
        /// <param name="insertClusivityBuilder">The clusivity builder for entities which will be inserted.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The result of the sync, containing which entities were inserted and deleted.</returns>
        public static async Task<ISyncWithoutUpdateResult<TEntity>> SyncWithoutUpdateAsync<TEntity>(
            this DbContext dbContext,
            Func<(IQueryable<TEntity> source, IQueryable<TEntity> target), IQueryable<TEntity>> targetResolver,
            IReadOnlyCollection<TEntity> source,
            IClusivityBuilder<TEntity> insertClusivityBuilder = null,
            CancellationToken cancellationToken = default)
            where TEntity : class, new()
            => await SyncInternalAsync(
                dbContext ?? throw new ArgumentNullException(nameof(dbContext)),
                target: null,
                targetResolver ?? throw new ArgumentNullException(nameof(targetResolver)),
                source ?? throw new ArgumentNullException(nameof(source)),
                ignoreUpdates: true, // this is a sync without updates
                ignoreDeletions: false,
                insertClusivityBuilder,
                null,
                cancellationToken);

        /// <summary>
        /// Upserts the <paramref name="source"/> into the <typeparamref name="TEntity"/>'s <see cref="DbSet{TEntity}"/>. This entails:
        /// 1. Inserting any entities which exist in source, but does not exist in the database.
        /// 2. Updating the properties of any entities which exist in both source and the database to the values found in source
        /// </summary>
        /// <typeparam name="TEntity">The entity type.</typeparam>
        /// <param name="dbContext">EF Db Context</param>
        /// <param name="source">The collection of entities to upsert from.</param>
        /// <param name="insertClusivityBuilder">The clusivity builder for entities which will be inserted.</param>
        /// <param name="updateClusivityBuilder">The clusivity builder for entities which will be updated. You may use this to only update a subset of properties.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>The result of the upsert, containing which entities were inserted and updated.</returns>
        public static async Task<IUpsertResult<TEntity>> UpsertAsync<TEntity>(this DbContext dbContext, IReadOnlyCollection<TEntity> source, IClusivityBuilder<TEntity> insertClusivityBuilder = null, IClusivityBuilder<TEntity> updateClusivityBuilder = null, CancellationToken cancellationToken = default)
            where TEntity : class, new()
            => await SyncInternalAsync(
                dbContext ?? throw new ArgumentNullException(nameof(dbContext)),
                target: dbContext.Set<TEntity>(),
                targetResolver: null,
                source ?? throw new ArgumentNullException(nameof(source)),
                ignoreUpdates: false,
                ignoreDeletions: true, // this is a sync for upserts
                insertClusivityBuilder,
                updateClusivityBuilder,
                cancellationToken);

        private static async Task<SyncResult<TEntity>> SyncInternalAsync<TEntity>(
            this DbContext dbContext,
            IQueryable<TEntity> target,
            Func<(IQueryable<TEntity> source, IQueryable<TEntity> target), IQueryable<TEntity>> targetResolver,
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

            var parameters = new List<object>(source.Count * properties.Length);

            bool isSqlite = dbContext.Database.IsSqlite();
            if (isSqlite)
            {
                stringBuilder.AddSqliteSyncCommand(
                    dbContext: dbContext,
                    entityType: entityType,
                    target: target,
                    targetResolver: targetResolver,
                    source: source,
                    ignoreUpdates: ignoreUpdates,
                    ignoreDeletions: ignoreDeletions,
                    primaryKey: primaryKey,
                    nonPrimaryKeyProperties: nonPrimaryKeyProperties,
                    properties: properties,
                    propertiesToUpdate: propertiesToUpdate,
                    propertiesToInsert: propertiesToInsert,
                    parameters: parameters);
            }
            else
            {
                await stringBuilder.AddSqlServerSyncCommand(
                    dbContext: dbContext,
                    entityType: entityType,
                    target: target,
                    targetResolver: targetResolver,
                    source: source,
                    ignoreUpdates: ignoreUpdates,
                    ignoreDeletions: ignoreDeletions,
                    primaryKey: primaryKey,
                    properties: properties,
                    nonPrimaryKeyProperties: nonPrimaryKeyProperties,
                    propertiesToUpdate: propertiesToUpdate,
                    propertiesToInsert: propertiesToInsert,
                    parameters: parameters,
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
            DbContext dbContext,
            IEntityType entityType,
            IQueryable<TEntity> target,
            Func<(IQueryable<TEntity> source, IQueryable<TEntity> target), IQueryable<TEntity>> targetResolver,
            IReadOnlyCollection<TEntity> source,
            bool ignoreUpdates,
            bool ignoreDeletions,
            IKey primaryKey,
            IProperty[] properties,
            IProperty[] nonPrimaryKeyProperties,
            IProperty[] propertiesToUpdate,
            IProperty[] propertiesToInsert,
            List<object> parameters)
            where TEntity : class, new()
        {
            string targetCommand = null;
            IReadOnlyCollection<System.Data.SqlClient.SqlParameter> targetCommandParameters;

            // If we got a resolver, we'll have to resolve the target.
            if (targetResolver != null)
            {
                IQueryable<TEntity> sourceAsQueryable = dbContext.Set<TEntity>().FromSqlRaw("SELECT * FROM source");

                target = targetResolver((sourceAsQueryable, dbContext.Set<TEntity>()));
                (targetCommand, targetCommandParameters) = target.ToSqlCommand(filterCollapsedP0Param: true);
            }
            else
            {
                (targetCommand, targetCommandParameters) = target.ToSqlCommand();
            }

            parameters.AddRange(targetCommandParameters);

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
                         .AppendLine("SELECT (CASE WHEN (")
                            .AppendJoin(" OR ", primaryKey.Properties.Select(property => FormattableString.Invariant($"target.{property.Name} IS NULL")))
                            .Append(") THEN 'INSERT' ELSE 'UPDATE' END) AS _$action, ")
                            .AppendColumnNames(properties, false, "source", SourceAliaser).Append(", ")
                            .AppendColumnNames(properties, false, "target", TargetAliaser)
                            .AppendLine("FROM source LEFT OUTER JOIN target ON ").AppendJoinCondition(primaryKey);

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
                        .Append("WHERE ").AppendJoin(" OR ", primaryKey.Properties.Select(property => FormattableString.Invariant($"source.{property.Name} IS NULL"))).AppendLine(";")
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
            IQueryable<TEntity> target,
            Func<(IQueryable<TEntity> source, IQueryable<TEntity> target), IQueryable<TEntity>> targetResolver,
            IReadOnlyCollection<TEntity> source,
            bool ignoreUpdates,
            bool ignoreDeletions,
            IKey primaryKey,
            IProperty[] properties,
            IProperty[] nonPrimaryKeyProperties,
            IProperty[] propertiesToUpdate,
            IProperty[] propertiesToInsert,
            List<object> parameters,
            CancellationToken cancellationToken)
            where TEntity : class, new()
        {
            ManipulationExtensionsConfiguration configuration = dbContext.GetConfiguration();

            string userDefinedTableTypeName = null;
            string tableValuedParameter = null;

            // There are three cases where we use a TVP as source:
            // 1. when we exceed the param thresholds set in configuration
            // 2. when we have a target resolver, and
            // 3. when we do a simple-statement sync (i.e. not using MERGE)
            if (configuration.SqlServerConfiguration.ShouldUseTableValuedParameters(properties, source)
                || targetResolver != null
                || !configuration.SqlServerConfiguration.ShouldUseMerge<TEntity>())
            {
                userDefinedTableTypeName = await dbContext.Database.CreateUserDefinedTableTypeIfNotExistsAsync(entityType, configuration.SqlServerConfiguration, cancellationToken);
                tableValuedParameter = SqlCommandBuilderExtensions.CreateTableValuedParameter(userDefinedTableTypeName, properties, source, parameters);
            }

            string targetCommand = null;
            IReadOnlyCollection<System.Data.SqlClient.SqlParameter> targetCommandParameters;

            // If we got a resolver, we'll have to resolve the target.
            if (targetResolver != null)
            {
                IQueryable<TEntity> sourceAsQueryable = dbContext.Set<TEntity>().FromSqlRaw(new StringBuilder().Append("SELECT * FROM ").Append(tableValuedParameter).ToString());

                target = targetResolver((sourceAsQueryable, dbContext.Set<TEntity>()));
                (targetCommand, targetCommandParameters) = target.ToSqlCommand(filterCollapsedP0Param: true);
            }
            else
            {
                (targetCommand, targetCommandParameters) = target.ToSqlCommand();
            }

            parameters.AddRange(targetCommandParameters);

            if (configuration.SqlServerConfiguration.ShouldUseMerge<TEntity>())
            {
                bool outputInto = configuration.SqlServerConfiguration.DoesEntityHaveTriggers<TEntity>();

                string outputType = null;
                if (outputInto)
                {
                    // The output type is the same as userDefinedTableTypeName, but with an aditional field for Action
                    outputType = await dbContext.Database.CreateUserDefinedTableTypeIfNotExistsAsync(entityType, configuration.SqlServerConfiguration, cancellationToken, includeActionColumn: true);
                    stringBuilder.AppendOutputDeclaration(outputType);
                }

                stringBuilder
                    .AppendLine("SET NOCOUNT ON;")
                    .AppendLine("WITH TargetData AS (").Append(targetCommand).AppendLine(")")
                    .AppendLine("MERGE INTO TargetData AS target ")
                    .Append("USING ");

                if (tableValuedParameter != null)
                {
                    stringBuilder.Append(tableValuedParameter);
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
                        .Append("OUTPUT ").AppendColumnNames(properties, wrapInParanthesis: false, "deleted").Append(" INTO ").AppendLine("@DeleteResult")
                        .Append("WHERE NOT EXISTS (SELECT 1 FROM ").Append(tableValuedParameter).Append(" source WHERE ").AppendJoinCondition(primaryKey).AppendLine(");");
                }

                // UPDATE
                if (!ignoreUpdates)
                {
                    stringBuilder
                        .Append("DECLARE @UpdateResult ").Append(userDefinedTableTypeName).AppendLine(";")
                        .Append("UPDATE ").Append(tableName).AppendLine(" SET")
                            .AppendJoin(",", propertiesToUpdate.Select(property => FormattableString.Invariant($"{property.Name}=source.{property.Name}"))).AppendLine()
                        .Append("OUTPUT ").AppendColumnNames(properties, wrapInParanthesis: false, "deleted").Append(" INTO ").AppendLine("@UpdateResult")
                        .Append("FROM ").Append(tableName).Append(" AS target INNER JOIN ").Append(tableValuedParameter).Append(" source ON ").AppendJoinCondition(primaryKey).AppendLine(";");
                }

                // INSERT if not exists in taget table
                string tableToCheckForExistence = ignoreUpdates ? tableName : "@UpdateResult";
                stringBuilder
                    .Append("INSERT INTO ").Append(tableName).AppendColumnNames(propertiesToInsert, wrapInParanthesis: true).AppendLine()
                    .Append("OUTPUT ").AppendColumnNames(properties, wrapInParanthesis: false, "inserted").Append(" INTO ").AppendLine("@InsertResult")
                    .Append("SELECT ").AppendJoin(",", propertiesToInsert.Select(m => m.GetColumnName())).AppendLine()
                    .Append("FROM ").Append(tableValuedParameter).AppendLine(" source")
                    .Append("WHERE NOT EXISTS (SELECT 1 FROM ")
                        .Append(tableToCheckForExistence).Append(" target WHERE ")
                        .AppendJoinCondition(primaryKey).AppendLine(");");

                // Return the sync result
                stringBuilder.Append("SELECT 'INSERT' AS __Action, ").AppendColumnNames(properties, wrapInParanthesis: false).AppendLine(" FROM @InsertResult");

                if (!ignoreUpdates)
                {
                    stringBuilder
                        .AppendLine("UNION")
                        .Append("SELECT 'UPDATE' AS __Action, ").AppendColumnNames(properties, wrapInParanthesis: false).AppendLine(" FROM @UpdateResult");
                }

                if (!ignoreDeletions)
                {
                    stringBuilder
                        .AppendLine("UNION")
                        .Append("SELECT 'DELETE' AS __Action,").AppendColumnNames(properties, wrapInParanthesis: false).AppendLine(" FROM @DeleteResult");
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
            stringBuilder.Append(actionColumnName).Append(", ");

            foreach (IProperty keyProperty in primaryKey.Properties)
            {
                string columnName = keyProperty.GetColumnName();
                stringBuilder.Append("CASE ").Append(actionColumnName)
                    .Append(" WHEN 'INSERT' THEN ")
                    .Append(insertedAliaser(columnName))
                    .Append(" ELSE ").Append(deletedAliaser(columnName))
                    .Append(" END AS ").Append(columnName).Append(',');
            }

            if (nonPrimaryKeyProperties.Count > 0)
            {
                stringBuilder.AppendJoin(',', nonPrimaryKeyProperties.Select(prop => deletedAliaser(prop.GetColumnName())));
            }
            else
            {
                stringBuilder.Length--; // Remove the last , if there are no additional properties
            }

            return stringBuilder;
        }
    }
}
