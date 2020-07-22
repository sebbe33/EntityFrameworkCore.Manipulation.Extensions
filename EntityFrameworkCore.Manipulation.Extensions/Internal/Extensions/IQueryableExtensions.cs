using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.Query.Internal;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.EntityFrameworkCore.Storage;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;

namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
    internal static class IQueryableExtensions
    {
        public static (string CommantText, IReadOnlyCollection<SqlParameter> Parameters) ToSqlCommand<TEntity>(this IQueryable<TEntity> query)
            where TEntity : class
        {
            IEnumerator<TEntity> enumerator = query.Provider.Execute<IEnumerable<TEntity>>(query.Expression).GetEnumerator();
            RelationalCommandCache relationalCommandCache = enumerator.Private("_relationalCommandCache") as RelationalCommandCache;
            RelationalQueryContext queryContext = enumerator.Private<RelationalQueryContext>("_relationalQueryContext");
            IReadOnlyDictionary<string, object> parameterValues = queryContext.ParameterValues;

            IRelationalCommand command;

            // For EF3.1. Credit: https://stackoverflow.com/questions/37527783/get-sql-code-from-an-entity-framework-core-iqueryablet/51583047#51583047
            if (relationalCommandCache != null)
            {
#pragma warning disable EF1001 // Internal EF Core API usage.
                command = relationalCommandCache.GetRelationalCommand(parameterValues);
#pragma warning restore EF1001 // Internal EF Core API usage.
            }
            else // For EF3.0
            {
                SelectExpression selectExpression = enumerator.Private<SelectExpression>("_selectExpression");
                IQuerySqlGeneratorFactory factory = enumerator.Private<IQuerySqlGeneratorFactory>("_querySqlGeneratorFactory");

                QuerySqlGenerator sqlGenerator = factory.Create();
                command = sqlGenerator.GetCommand(selectExpression);
            }

            var sqlParams = command.Parameters.Select(param => new SqlParameter($"@{param.InvariantName}", parameterValues[param.InvariantName])).ToArray();

            return (command.CommandText, sqlParams);
        }

        private static readonly BindingFlags bindingFlags = BindingFlags.Instance | BindingFlags.NonPublic;

        private static object Private(this object obj, string privateField) => obj?.GetType().GetField(privateField, bindingFlags)?.GetValue(obj);

        private static T Private<T>(this object obj, string privateField) => (T)obj?.GetType().GetField(privateField, bindingFlags)?.GetValue(obj) ?? throw new InvalidOperationException($"Cannot access {privateField}.");
    }
}
