namespace EntityFrameworkCore.Manipulation.Extensions
{
    using EntityFrameworkCore.Manipulation.Extensions.Internal;
    using EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Metadata;
    using System;
    using System.Collections.Generic;
    using Microsoft.Data.SqlClient;
    using System.Linq;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public static class DeleteExtensions
    {
        /// <summary>
        /// Deletes all entities in a target collection (<paramref name="deleteTarget"/>),
        /// and returns the deleted entities.
        /// </summary>
        /// <typeparam name="TEntity">The type of entity.</typeparam>
        /// <param name="dbContext">The <see cref="DbContext"/> to operate on.</param>
        /// <param name="deleteTarget">The target of entities to delete.</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>The collection of entities which were deleted.</returns>
        public static Task<IReadOnlyCollection<TEntity>> DeleteAsync<TEntity>(this DbContext dbContext, IQueryable<TEntity> deleteTarget, CancellationToken cancellationToken = default)
            where TEntity : class
            => DeleteInternalAsync(
                dbContext ?? throw new ArgumentNullException(nameof(dbContext)),
                deleteTarget ?? throw new ArgumentNullException(nameof(deleteTarget)),
                cancellationToken);

        private static async Task<IReadOnlyCollection<TEntity>> DeleteInternalAsync<TEntity>(this DbContext dbContext, IQueryable<TEntity> deleteTarget, CancellationToken cancellationToken)
            where TEntity : class
        {
            (string targetCommand, IReadOnlyCollection<SqlParameter> targetCommandParameters) = deleteTarget.ToSqlCommand();

            var stringBuilder = new StringBuilder(300);

            if (dbContext.Database.IsSqlite())
            {
                const string TempDeleteTableName = "EntityFrameworkManipulationDelete";
                IEntityType entityType = dbContext.Model.FindEntityType(typeof(TEntity));

                string tableName = entityType.GetTableName();
                IKey primaryKey = entityType.FindPrimaryKey();

                stringBuilder.AppendLine("BEGIN TRANSACTION;")
                             .Append("DROP TABLE IF EXISTS ").Append(TempDeleteTableName).AppendLine(";")
                             .Append("CREATE TEMP TABLE ").Append(TempDeleteTableName).Append(" AS ").Append(targetCommand).AppendLine(";");

                stringBuilder
                    .Append("DELETE FROM ").AppendLine(tableName)
                    .Append("WHERE EXISTS (SELECT 1 FROM ").Append(TempDeleteTableName).Append(" WHERE ").AppendJoinCondition(primaryKey, tableName, TempDeleteTableName).AppendLine(");");

                stringBuilder.Append("SELECT * FROM ").Append(TempDeleteTableName).AppendLine(";");
                stringBuilder.AppendLine("COMMIT;");
            }
            else
            {
                stringBuilder
                    .AppendLine("SET NOCOUNT ON;")
                    .AppendLine("WITH DeleteTarget AS (").Append(targetCommand).AppendLine(")")
                    .AppendLine("DELETE FROM DeleteTarget ")
                    .AppendLine("OUTPUT DELETED.* ");
            }

            object[] parameters = targetCommandParameters.ToArray();

            return await dbContext.Set<TEntity>().FromSqlRaw(stringBuilder.ToString(), parameters)
                                                .AsNoTracking()
                                                .ToListAsync(cancellationToken)
                                                .ConfigureAwait(false);
        }
    }
}
