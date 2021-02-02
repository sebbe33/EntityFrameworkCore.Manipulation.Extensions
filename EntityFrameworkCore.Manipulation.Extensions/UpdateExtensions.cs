using EntityFrameworkCore.Manipulation.Extensions.Internal;
using EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace EntityFrameworkCore.Manipulation.Extensions
{
	public class UpdateEntry<TEntity>
		where TEntity : class, new()
	{
		public TEntity Current { get; set; }

		public TEntity Incoming { get; set; }
	}

	public static class UpdateExtensions
	{
		private static readonly Regex joinAliasRegex = new Regex("(?<=AS).+(?=ON)");

		public static Task<IReadOnlyCollection<TEntity>> UpdateAsync<TEntity>(
			this DbContext dbContext,
			IReadOnlyCollection<TEntity> source,
			IEnumerable<Expression<Func<TEntity, object>>> includedProperties = null,
			Expression<Func<UpdateEntry<TEntity>, bool>> condition = null,
			CancellationToken cancellationToken = default)
			where TEntity : class, new()
		{
			if (dbContext == null)
			{
				throw new ArgumentNullException(nameof(dbContext));
			}

			if (source == null)
			{
				throw new ArgumentNullException(nameof(source));
			}

			if (source.Count == 0)
			{
				// Nothing to do.
				return Task.FromResult<IReadOnlyCollection<TEntity>>(Array.Empty<TEntity>());
			}

			return UpdateInternalAsync(dbContext, source, includedProperties, condition, cancellationToken);
		}

		private static async Task<IReadOnlyCollection<TEntity>> UpdateInternalAsync<TEntity>(
			this DbContext dbContext,
			IReadOnlyCollection<TEntity> source,
			IEnumerable<Expression<Func<TEntity, object>>> includedPropertyExpressions,
			Expression<Func<UpdateEntry<TEntity>, bool>> condition,
			CancellationToken cancellationToken)
			where TEntity : class, new()
		{
			var stringBuilder = new StringBuilder(1000);

			IEntityType entityType = dbContext.Model.FindEntityType(typeof(TEntity));

			string tableName = entityType.GetTableName();
			IKey primaryKey = entityType.FindPrimaryKey();
			IProperty[] properties = entityType.GetProperties().ToArray();
			IProperty[] nonPrimaryKeyProperties = properties.Except(primaryKey.Properties).ToArray();

			IProperty[] propertiesToUpdate = nonPrimaryKeyProperties;
			if (includedPropertyExpressions != null)
			{
				// If there's a selection of properties to include, we'll filter it down to that + the PK
				propertiesToUpdate = properties
					.Intersect(primaryKey.Properties.Concat(includedPropertyExpressions.GetPropertiesFromExpressions(properties)).Distinct())
					.ToArray();
			}

			List<object> parameters = new List<object>();

			var isSqlite = dbContext.Database.IsSqlite();
			if (isSqlite)
			{
				string incomingInlineTableCommand = new StringBuilder().AppendSelectFromInlineTable(properties, source, parameters, "x", sqliteSyntax: true).ToString();
				IQueryable<TEntity> incoming = CreateIncomingQueryable(dbContext, incomingInlineTableCommand, condition, parameters);
				(string sourceCommand, var sourceCommandParameters) = incoming.ToSqlCommand(filterCollapsedP0Param: true);
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
				string userDefinedTableTypeName = null;
				if (ConfigUtils.ShouldUseTableValuedParameters(properties, source))
				{
					userDefinedTableTypeName = await dbContext.Database.CreateUserDefinedTableTypeIfNotExistsAsync(entityType, cancellationToken);
				}

				string incomingInlineTableCommand = userDefinedTableTypeName != null ?
					new StringBuilder().Append("SELECT * FROM ").AppendTableValuedParameter(userDefinedTableTypeName, properties, source, parameters).ToString()
					:
					new StringBuilder().AppendSelectFromInlineTable(properties, source, parameters, "x").ToString();

				IQueryable<TEntity> incoming = CreateIncomingQueryable(dbContext, incomingInlineTableCommand, condition, parameters);

				(string sourceCommand, var sourceCommandParameters) = incoming.ToSqlCommand(filterCollapsedP0Param: true);
				parameters.AddRange(sourceCommandParameters);

				// Here's where we have to cheat a bit to get an efficient query. If we were to place the sourceCommand in a CTE,
				// then join onto that CTE in the UPADATE, then the query optimizer can't handle mergining the looksups, and it will do two lookups,
				// one for the CTE and one for the UPDATE JOIN. Instead, we'll just pick everything put the SELECT part of the sourceCommand and
				// attach it to the UPDATE command, which works since it follows the exact format of a SELECT, except for the actual selecting of properties.
				string fromJoinCommand = sourceCommand.Substring(sourceCommand.IndexOf("FROM"));

				// Get the alias of the inline table in the source command
				string inlineTableAlias = joinAliasRegex.Match(fromJoinCommand).Value.Trim();

				stringBuilder
					.Append("UPDATE ").Append(tableName).AppendLine(" SET")
						.AppendJoin(",", propertiesToUpdate.Select(property => FormattableString.Invariant($"{property.Name}={inlineTableAlias}.{property.Name}"))).AppendLine()
					.AppendLine("OUTPUT inserted.*")
					.Append(fromJoinCommand);
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
			where TEntity : class, new()
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
