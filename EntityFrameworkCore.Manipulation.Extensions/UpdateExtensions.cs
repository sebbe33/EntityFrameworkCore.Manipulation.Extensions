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
	public class UpdateEntry<TEntity, TInlineEntity>
		where TEntity : class, new()
	{
		public TEntity Current { get; set; }

		public TInlineEntity Incoming { get; set; }
	}

	public static class UpdateExtensions
	{
		private static readonly Regex joinAliasRegex = new Regex("(?<=AS).+(?=ON)");

		public static Task<IReadOnlyCollection<TEntity>> UpdateAsync<TEntity>(
			this DbContext dbContext,
			IReadOnlyCollection<TEntity> source,
			IEnumerable<Expression<Func<TEntity, object>>> includedProperties = null,
			Expression<Func<UpdateEntry<TEntity, TEntity>, bool>> condition = null,
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

			return UpdateInternalAsync<TEntity, TEntity, TEntity>(dbContext, source, includedProperties, condition, cancellationToken);
		}

		public static Task<IReadOnlyCollection<TEntity>> UpdateAsync<TEntity, TInlineEntity, TBase>(
			this DbContext dbContext,
			IReadOnlyCollection<TInlineEntity> source,
			IEnumerable<Expression<Func<TEntity, object>>> includedProperties = null,
			Expression<Func<UpdateEntry<TEntity, TInlineEntity>, bool>> condition = null,
			CancellationToken cancellationToken = default)
			where TEntity : class, TBase, new()
			where TInlineEntity : class, TBase, new()
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

			return UpdateInternalAsync<TEntity, TInlineEntity, TBase>(dbContext, source, includedProperties, condition, cancellationToken);
		}

		private static async Task<IReadOnlyCollection<TEntity>> UpdateInternalAsync<TEntity, TInlineEntity, TBase>(
			this DbContext dbContext,
			IReadOnlyCollection<TInlineEntity> source,
			IEnumerable<Expression<Func<TEntity, object>>> includedPropertyExpressions,
			Expression<Func<UpdateEntry<TEntity, TInlineEntity>, bool>> condition,
			CancellationToken cancellationToken)
			where TEntity : class, TBase, new()
			where TInlineEntity : class, TBase, new()
		{
			var stringBuilder = new StringBuilder(1000);

			IEntityType entityType = dbContext.Model.FindEntityType(typeof(TEntity));
			IEntityType inlineEntityType = dbContext.Model.FindEntityType(typeof(TInlineEntity));

			string targetTableName = entityType.GetTableName();
			IKey primaryKey = entityType.FindPrimaryKey();
			IProperty[] targetTableProperties = entityType.GetProperties().ToArray();
			IProperty[] targetTableNonPrimaryKeyProperties = targetTableProperties.Except(primaryKey.Properties).ToArray();

			IProperty[] inlineTableProperties = inlineEntityType.GetProperties().ToArray();

			IProperty[] propertiesToUpdate = targetTableNonPrimaryKeyProperties;
			if (includedPropertyExpressions != null)
			{
				// If there's a selection of properties to include, we'll filter it down to that + the PK
				propertiesToUpdate = targetTableProperties
					.Intersect(primaryKey.Properties.Concat(includedPropertyExpressions.GetPropertiesFromExpressions(targetTableProperties)).Distinct())
					.ToArray();
			}

			List<object> parameters = new List<object>();

			var isSqlite = dbContext.Database.IsSqlite();
			if (isSqlite)
			{
				string incomingInlineTableCommand = new StringBuilder().AppendSelectFromInlineTable(inlineTableProperties, source, parameters, "x", sqliteSyntax: true).ToString();
				IQueryable<TInlineEntity> incoming = CreateIncomingQueryable<TEntity, TInlineEntity, TBase>(dbContext, incomingInlineTableCommand, condition, parameters);
				(string sourceCommand, var sourceCommandParameters) = incoming.ToSqlCommand(filterCollapsedP0Param: true);
				parameters.AddRange(sourceCommandParameters);

				const string TempDeleteTableName = "EntityFrameworkManipulationUpdate";

				// Create temp table with the applicable items to be update
				stringBuilder.AppendLine("BEGIN TRANSACTION;")
							 .Append("DROP TABLE IF EXISTS ").Append(TempDeleteTableName).AppendLine(";")
							 .Append("CREATE TEMP TABLE ").Append(TempDeleteTableName).Append(" AS ")
							 .Append(sourceCommand).AppendLine(";");

				// Update the target table from the temp table
				stringBuilder.Append("UPDATE ").Append(targetTableName).AppendLine(" SET")
						.AppendJoin(",", propertiesToUpdate.Select(property => FormattableString.Invariant($"{property.Name}=incoming.{property.Name}"))).AppendLine()
						.Append("FROM ").Append(TempDeleteTableName).AppendLine(" AS incoming")
						.Append("WHERE ").AppendJoinCondition(primaryKey, targetTableName, "incoming").AppendLine("; ");

				// Select the latest state of the affected rows in the table
				stringBuilder
					.Append("SELECT target.* FROM ").Append(targetTableName).AppendLine(" AS target")
						.Append(" JOIN ").Append(TempDeleteTableName).Append(" AS source ON ").AppendJoinCondition(primaryKey).AppendLine(";")
					.Append("COMMIT;");
			}
			else
			{
				string userDefinedTableTypeName = null;
				if (ConfigUtils.ShouldUseTableValuedParameters(inlineTableProperties, source))
				{
					userDefinedTableTypeName = await dbContext.Database.CreateUserDefinedTableTypeIfNotExistsAsync(inlineEntityType, cancellationToken);
				}

				string incomingInlineTableCommand = userDefinedTableTypeName != null ?
					new StringBuilder().AppendTableValuedParameter(userDefinedTableTypeName, inlineTableProperties, source, parameters).ToString()
					:
					new StringBuilder().AppendSelectFromInlineTable(inlineTableProperties, source, parameters, "x").ToString();

				IQueryable<TInlineEntity> incoming = CreateIncomingQueryable<TEntity, TInlineEntity, TBase>(dbContext, incomingInlineTableCommand, condition, parameters);

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
					.Append("UPDATE ").Append(targetTableName).AppendLine(" SET")
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

		private static IQueryable<TInlineEntity> CreateIncomingQueryable<TEntity, TInlineEntity, TBase>(
			DbContext dbContext,
			string incomingInlineTableCommand,
			Expression<Func<UpdateEntry<TEntity, TInlineEntity>, bool>> condition,
			List<object> parameters)
			where TEntity : class, TBase, new()
			where TInlineEntity : class, TBase, new()
		{
			IQueryable<TInlineEntity> incoming;

			// Create the incoming query as an inline table joined onto the target table
			if (condition != null)
			{
				incoming = dbContext.Set<TEntity>()
					.Join(
						dbContext.Set<TInlineEntity>().FromSqlRaw(incomingInlineTableCommand, parameters.ToArray()),
						x => (TBase)x,
						x => (TBase)x,
						(outer, inner) => new UpdateEntry<TEntity, TInlineEntity> { Current = outer, Incoming = inner })
					.Where(condition)
					.Select(updateEntry => updateEntry.Incoming);
			}
			else
			{
				incoming = dbContext.Set<TEntity>()
					.Join(
						dbContext.Set<TInlineEntity>().FromSqlRaw(incomingInlineTableCommand, parameters.ToArray()),
						x => (TBase)x,
						x => (TBase)x,
						(outer, inner) => inner);
			}

			return incoming;
		}
	}
}
