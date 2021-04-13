namespace EntityFrameworkCore.Manipulation.Extensions
{
    using EntityFrameworkCore.Manipulation.Extensions.Configuration;
    using EntityFrameworkCore.Manipulation.Extensions.Configuration.Internal;
    using EntityFrameworkCore.Manipulation.Extensions.Internal;
    using EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Metadata;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public static class InsertIfNotExistExtensions
    {
        /// <summary>
        /// Inserts a collection of <paramref name="entities"/> into the given <paramref name="dbSet"/>,
        /// and returns the inserted entities. If an entity already exists, it will be skipped.
        /// </summary>
        /// <typeparam name="TEntity">The type of entity.</typeparam>
        /// <param name="dbContext">The <see cref="DbContext"/> to operate on.</param>
        /// <param name="dbSet">The <see cref="DbSet{TEntity}"/> to insert into.</param>
        /// <param name="entities">The entities to insert.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The collection of entities which were inserted.</returns>
        public static Task<IReadOnlyCollection<TEntity>> InsertIfNotExistAsync<TEntity>(this DbContext dbContext, DbSet<TEntity> dbSet, IReadOnlyCollection<TEntity> entities, CancellationToken cancellationToken = default)
            where TEntity : class
            => InsertIfNotExistInternalAsync(
                dbContext ?? throw new ArgumentNullException(nameof(dbContext)),
                dbSet ?? throw new ArgumentNullException(nameof(dbSet)),
                entities ?? throw new ArgumentNullException(nameof(entities)),
                cancellationToken);

        private static async Task<IReadOnlyCollection<TEntity>> InsertIfNotExistInternalAsync<TEntity>(this DbContext dbContext, DbSet<TEntity> dbSet, IReadOnlyCollection<TEntity> entities, CancellationToken cancellationToken)
            where TEntity : class
        {
            if (entities.Count == 0)
            {
                return new TEntity[0];
            }

            ManipulationExtensionsConfiguration configuration = dbContext.GetConfiguration();

            IEntityType entityType = dbContext.Model.FindEntityType(typeof(TEntity));

            string tableName = entityType.GetSchemaQualifiedTableName();
            IKey primaryKey = entityType.FindPrimaryKey();
            IProperty[] properties = entityType.GetProperties().ToArray();

            IList<object> parameters = new List<object>(entities.Count * properties.Length);

            var stringBuilder = new StringBuilder((parameters.Count * 4) + 300); // every param on average takes up 4 char + ~300 for the rest of the fairly static query

            if (dbContext.Database.IsSqlite())
            {
                stringBuilder.AppendLine("BEGIN TRANSACTION;")
                             .AppendLine("DROP TABLE IF EXISTS EntityFrameworkManipulationInsertIfNotExists;")
                             .Append("CREATE TEMP TABLE EntityFrameworkManipulationInsertIfNotExists AS ")
                             .AppendSelectFromInlineTable(properties, entities, parameters, "source", sqliteSyntax: true).AppendLine("WHERE NOT EXISTS (");

                // sub-query to filter out entities which already exist
                stringBuilder.Append("SELECT 1 FROM ").Append(tableName).Append(" AS target WHERE ").AppendJoinCondition(primaryKey).AppendLine(");");

                stringBuilder
                    .Append("INSERT INTO ").Append(tableName).AppendColumnNames(properties, wrapInParanthesis: true).Append("SELECT * FROM EntityFrameworkManipulationInsertIfNotExists;")
                    .AppendLine("SELECT * FROM EntityFrameworkManipulationInsertIfNotExists;")
                    .Append("COMMIT;");
            }
            else
            {
                string userDefinedTableTypeName = null;
                if (configuration.SqlServerConfiguration.ShouldUseTableValuedParameters(properties, entities))
                {
                    userDefinedTableTypeName = await dbContext.Database.CreateUserDefinedTableTypeIfNotExistsAsync(entityType, configuration.SqlServerConfiguration, cancellationToken);
                }

                stringBuilder.AppendLine("INSERT INTO ").Append(tableName).AppendColumnNames(properties, wrapInParanthesis: true)
                             .AppendLine("OUTPUT ").AppendColumnNames(properties, false, "inserted");

                if (configuration.SqlServerConfiguration.ShouldUseTableValuedParameters(properties, entities))
                {
                    stringBuilder.Append("SELECT * FROM ")
                        .AppendTableValuedParameter(userDefinedTableTypeName, properties, entities, parameters)
                        .AppendLine(" AS source");
                }
                else
                {
                    stringBuilder.AppendSelectFromInlineTable(properties, entities, parameters, "source");
                }

                stringBuilder.Append("WHERE NOT EXISTS (");

                // sub-query to filter out entities which already exist
                stringBuilder.Append("SELECT 1 FROM ").Append(tableName).Append(" AS target WHERE ")
                    .AppendJoin(" AND ", primaryKey.Properties.Select(property => FormattableString.Invariant($"target.{property.Name}=source.{property.Name}")))
                    .Append(");");
            }

            return await dbSet.FromSqlRaw(stringBuilder.ToString(), parameters.ToArray())
                                                .AsNoTracking()
                                                .ToListAsync(cancellationToken)
                                                .ConfigureAwait(false);
        }
    }
}
