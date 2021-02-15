using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using EntityFrameworkCore.Manipulation.Extensions.Internal;
using EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EntityFrameworkCore.Manipulation.Extensions
{
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

            string tableName = entityType.GetTableName();
            IKey primaryKey = entityType.FindPrimaryKey();
            IProperty[] properties = entityType.GetProperties().ToArray();
            IProperty[] nonPrimaryKeyProperties = properties.Except(primaryKey.Properties).ToArray();

            IProperty[] propertiesToUpdate = updateClusivityBuilder == null ? nonPrimaryKeyProperties : updateClusivityBuilder.Build(nonPrimaryKeyProperties);
            IProperty[] propertiesToInsert = insertClusivityBuilder == null ? properties : primaryKey.Properties.Concat(insertClusivityBuilder.Build(nonPrimaryKeyProperties)).ToArray();

            (string targetCommand, var targetCommandParameters) = target.ToSqlCommand();

            List<object> parameters = new List<object>(source.Count * properties.Length);
            parameters.AddRange(targetCommandParameters);

            var isSqlite = dbContext.Database.IsSqlite();
            if (isSqlite)
            {
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
                stringBuilder.Append("SELECT _$action, ")
                        .AppendJoin(", ", primaryKey.Properties.Select(m => SourceAliaser(m.GetColumnName()))).Append(", ")
                        .AppendJoin(", ", properties.Select(m => TargetAliaser(m.GetColumnName())))
                        .AppendLine(" FROM EntityFrameworkManipulationSync;")
                    .Append("COMMIT;");
            }
            else
            {
                string userDefinedTableTypeName = null;
                if (ConfigUtils.ShouldUseTableValuedParameters(properties, source))
                {
                    userDefinedTableTypeName = await dbContext.Database.CreateUserDefinedTableTypeIfNotExistsAsync(entityType, cancellationToken);
                }

                stringBuilder
                    .AppendLine("WITH TargetData AS (").Append(targetCommand).AppendLine(")")
                    .AppendLine("MERGE INTO TargetData AS target ")
                    .Append("USING ");

                if (ConfigUtils.ShouldUseTableValuedParameters(properties, source))
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
                    .Append("OUTPUT $action, ")
                    .AppendColumnNames(primaryKey.Properties, wrapInParanthesis: false, "inserted").Append(", ")
                    .AppendColumnNames(properties, wrapInParanthesis: false, "deleted").Append(";");
            }

            using var reader = await dbContext.Database.ExecuteSqlQueryAsync(stringBuilder.ToString(), parameters.ToArray(), cancellationToken);

            List<TEntity> insertedEntities = new List<TEntity>();
            List<TEntity> deletedEntities = new List<TEntity>();
            List<(TEntity OldValue, TEntity NewValue)> updatedEntities = new List<(TEntity OldValue, TEntity NewValue)>();
            var deletedColumnOffset = 1 + primaryKey.Properties.Count; // action + PK key properties lengths

            var propertyValueConverters = isSqlite ? EntityUtils.GetEntityPropertiesValueConverters(properties) : null;
            var keyValueConverters = isSqlite ? EntityUtils.GetEntityPropertiesValueConverters(primaryKey.Properties.ToArray()) : null;

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
                var row = new object[1 + primaryKey.Properties.Count + properties.Length];
                reader.DbDataReader.GetValues(row);

                var action = row[0] as string;
                var insertKeyPropertyValues = row.Skip(1).Take(primaryKey.Properties.Count).ToArray();

                if (string.Equals(action, "INSERT", StringComparison.OrdinalIgnoreCase))
                {
					var newValue = EntityUtils.FindEntityBasedOnKey(source, primaryKey, insertKeyPropertyValues, keyValueConverters);
					if (propertiesNotIncludedInInsert != null)
					{
						// If properties are included/excluded from the insert, then we have to set them to their default value to reflect the state in the DB
						foreach (var property in propertiesNotIncludedInInsert)
						{
							property.PropertyInfo.SetValue(newValue, null);
						}
					}

					insertedEntities.Add(newValue);
                }
                else if (string.Equals(action, "DELETE", StringComparison.OrdinalIgnoreCase))
                {
                    deletedEntities.Add(EntityUtils.EntityFromRow<TEntity>(row, properties, deletedColumnOffset, propertyValueConverters));
                }
                else
                {
					// Update
					var oldValue = EntityUtils.EntityFromRow<TEntity>(row, properties, deletedColumnOffset, propertyValueConverters);
					var newValue = EntityUtils.FindEntityBasedOnKey(source, primaryKey, insertKeyPropertyValues, keyValueConverters);

					// We have to bear in mind that properties might be included/excluded. In that case, we'll have to take these props' values from the oldValue
					if (propertiesNotIncludedInUpdate != null)
					{
						foreach (var property in propertiesNotIncludedInUpdate)
						{
							property.PropertyInfo.SetValue(newValue, property.PropertyInfo.GetValue(oldValue));
						}
					}

                    updatedEntities.Add((oldValue, newValue));
                }
            }

            return new SyncResult<TEntity>(deletedEntities, insertedEntities, updatedEntities);
        }
    }
}
