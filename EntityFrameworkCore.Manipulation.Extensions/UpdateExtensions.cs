namespace EntityFrameworkCore.Manipulation.Extensions
{
    using System;
    using System.Collections.Generic;
    using Microsoft.Data.SqlClient;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Text;
    using System.Text.RegularExpressions;
    using System.Threading;
    using System.Threading.Tasks;
    using EntityFrameworkCore.Manipulation.Extensions.Configuration;
    using EntityFrameworkCore.Manipulation.Extensions.Configuration.Internal;
    using EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Metadata;

    public class UpdateEntry<TEntity>
        where TEntity : class
    {
        public TEntity Current { get; set; }

        public TEntity Incoming { get; set; }
    }

    public static class UpdateExtensions
    {
        private static readonly Regex joinAliasRegex = new Regex("(?<=AS).+(?=ON)");

        /// <summary>
        /// Conditionally updates the given <paramref name="source"/> transcationally.
        /// </summary>
        /// <typeparam name="TEntity">The type of entity to update.</typeparam>
        /// <param name="dbContext">The EF <see cref="DbContext"/>.</param>
        /// <param name="source">The local source of the update. These entities will update their corresponding match in the database if one exists.</param>
        /// <param name="condition">
        /// Optional: The condition on which to go through with an update on an entity level. You can use this to perform checks against
        /// the version of the entity that's in the database and determine if you want to update it. For example, you could
        /// update only if the version you're trying to update with is newer:
        /// <c>updateEntry => updateEntry.Current.Version < updateEntry.Incoming.Version</c>.
        /// </param>
        /// <param name="clusivityBuilder">
        /// A builder for included/excluded properties to update. This is used to target which properties to update. If no builder
        /// is supplied, all non-primary key properties will be updated.
        /// </param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// A collection of the entities which were updated. If for example an entity is missing in the DB,
        /// or it is not matching a the given condition, it will not be upate and not be included here.
        /// </returns>
        public static Task<IReadOnlyCollection<TEntity>> UpdateAsync<TEntity>(
            this DbContext dbContext,
            IReadOnlyCollection<TEntity> source,
            Expression<Func<UpdateEntry<TEntity>, bool>> condition = null,
            IClusivityBuilder<TEntity> clusivityBuilder = null,
            CancellationToken cancellationToken = default)
            where TEntity : class
        {
            if (dbContext == null)
            {
                throw new ArgumentNullException(nameof(dbContext));
            }

            if (source == null)
            {
                throw new ArgumentNullException(nameof(source));
            }

            return UpdateInternalAsync(dbContext, source, condition, clusivityBuilder, cancellationToken);
        }

        private static async Task<IReadOnlyCollection<TEntity>> UpdateInternalAsync<TEntity>(
            DbContext dbContext,
            IReadOnlyCollection<TEntity> source,
            Expression<Func<UpdateEntry<TEntity>, bool>> condition,
            IClusivityBuilder<TEntity> clusivityBuilder,
            CancellationToken cancellationToken)
            where TEntity : class
        {
            if (source.Count == 0)
            {
                // Nothing to do
                return Array.Empty<TEntity>();
            }

            ManipulationExtensionsConfiguration configuration = dbContext.GetConfiguration();
            var stringBuilder = new StringBuilder(1000);

            IEntityType entityType = dbContext.Model.FindEntityType(typeof(TEntity));
            string tableName = entityType.GetSchemaQualifiedTableName();
            IKey primaryKey = entityType.FindPrimaryKey();
            IProperty[] properties = entityType.GetProperties().ToArray();
            IProperty[] nonPrimaryKeyProperties = properties.Except(primaryKey.Properties).ToArray();

            IProperty[] propertiesToUpdate = clusivityBuilder == null ? nonPrimaryKeyProperties : clusivityBuilder.Build(nonPrimaryKeyProperties);

            var parameters = new List<object>();

            bool isSqlite = dbContext.Database.IsSqlite();
            if (isSqlite)
            {
                string incomingInlineTableCommand = new StringBuilder().AppendSelectFromInlineTable(properties, source, parameters, "x", sqliteSyntax: true).ToString();
                IQueryable<TEntity> incoming = CreateIncomingQueryable(dbContext, incomingInlineTableCommand, condition, parameters);
                (string sourceCommand, IReadOnlyCollection<SqlParameter> sourceCommandParameters) = incoming.ToSqlCommand(filterCompositeRelationParameter: true);
                parameters.AddRange(sourceCommandParameters);

                const string TempDeleteTableName = "EntityFrameworkManipulationUpdate";

                // Create temp table with the applicable items to be update
                stringBuilder.AppendLine("BEGIN TRANSACTION;")
                             .Append("DROP TABLE IF EXISTS ").Append(TempDeleteTableName).AppendLine(";")
                             .Append("CREATE TEMP TABLE ").Append(TempDeleteTableName).Append(" AS ")
                             .Append(sourceCommand).AppendLine(";");

                // Update the target table from the temp table
                stringBuilder.Append("UPDATE ").Append(tableName).AppendLine(" SET")
                        .AppendJoin(",", propertiesToUpdate.Select(property => FormattableString.Invariant($"{property.Name}=incoming.{property.Name}"))).AppendLine()
                        .Append("FROM ").Append(TempDeleteTableName).AppendLine(" AS incoming")
                        .Append("WHERE ").AppendJoinCondition(primaryKey, tableName, "incoming").AppendLine("; ");

                // Select the latest state of the affected rows in the table
                stringBuilder
                    .Append("SELECT target.* FROM ").Append(tableName).AppendLine(" AS target")
                        .Append(" JOIN ").Append(TempDeleteTableName).Append(" AS source ON ").AppendJoinCondition(primaryKey).AppendLine(";")
                    .Append("COMMIT;");
            }
            else
            {
                bool outputInto = configuration.SqlServerConfiguration.DoesEntityHaveTriggers<TEntity>();

                string userDefinedTableTypeName = null;
                if (configuration.SqlServerConfiguration.ShouldUseTableValuedParameters(properties, source) || outputInto)
                {
                    userDefinedTableTypeName = await dbContext.Database.CreateUserDefinedTableTypeIfNotExistsAsync(entityType, configuration.SqlServerConfiguration, cancellationToken);
                }

                if (outputInto)
                {
                    stringBuilder.AppendOutputDeclaration(userDefinedTableTypeName);
                }

                string incomingInlineTableCommand = userDefinedTableTypeName != null ?
                    new StringBuilder().Append("SELECT * FROM ").AppendTableValuedParameter(userDefinedTableTypeName, properties, source, parameters).ToString()
                    :
                    new StringBuilder().AppendSelectFromInlineTable(properties, source, parameters, "x").ToString();

                IQueryable<TEntity> incoming = CreateIncomingQueryable(dbContext, incomingInlineTableCommand, condition, parameters);

                (string sourceCommand, IReadOnlyCollection<SqlParameter> sourceCommandParameters) = incoming.ToSqlCommand(filterCompositeRelationParameter: true);
                parameters.AddRange(sourceCommandParameters);

                // Here's where we have to cheat a bit to get an efficient query. If we were to place the sourceCommand in a CTE,
                // then join onto that CTE in the UPADATE, then the query optimizer can't handle mergining the looksups, and it will do two lookups,
                // one for the CTE and one for the UPDATE JOIN. Instead, we'll just pick everything put the SELECT part of the sourceCommand and
                // attach it to the UPDATE command, which works since it follows the exact format of a SELECT, except for the actual selecting of properties.
                string fromJoinCommand = sourceCommand[sourceCommand.IndexOf("FROM")..];

                // Get the alias of the inline table in the source command
                string inlineTableAlias = joinAliasRegex.Match(fromJoinCommand).Value.Trim();

                stringBuilder
                    .AppendLine("SET NOCOUNT ON;")
                    .Append("UPDATE ").Append(tableName).AppendLine(" SET")
                        .AppendJoin(",", propertiesToUpdate.Select(property => FormattableString.Invariant($"{property.Name}={inlineTableAlias}.{property.Name}"))).AppendLine()
                    .AppendOutputClauseLine(properties, outputInto, identifierPrefix: "inserted")
                    .Append(fromJoinCommand)
                    .AppendLine(";");

                if (outputInto)
                {
                    stringBuilder.AppendOutputSelect(properties).AppendLine(";");
                }
            }

            return await dbContext.Set<TEntity>()
                .FromSqlRaw(
                    stringBuilder.ToString(),
                    parameters.ToArray())
                .AsNoTracking()
                .ToListAsync(cancellationToken);
        }

        private static IQueryable<TEntity> CreateIncomingQueryable<TEntity>(
            DbContext dbContext,
            string incomingInlineTableCommand,
            Expression<Func<UpdateEntry<TEntity>, bool>> condition,
            List<object> parameters)
            where TEntity : class
        {
            IQueryable<TEntity> incoming;

            // Create the incoming query as an inline table joined onto the target table
            if (condition != null)
            {
                incoming = dbContext.Set<TEntity>()
                    .Join(
                        dbContext.Set<TEntity>().FromSqlRaw(incomingInlineTableCommand, parameters.ToArray()),
                        x => x,
                        x => x,
                        (outer, inner) => new UpdateEntry<TEntity> { Current = outer, Incoming = inner })
                    .Where(condition)
                    .Select(updateEntry => updateEntry.Incoming);
            }
            else
            {
                incoming = dbContext.Set<TEntity>()
                    .Join(
                        dbContext.Set<TEntity>().FromSqlRaw(incomingInlineTableCommand, parameters.ToArray()),
                        x => x,
                        x => x,
                        (outer, inner) => inner);
            }

            return incoming;
        }
    }
}
