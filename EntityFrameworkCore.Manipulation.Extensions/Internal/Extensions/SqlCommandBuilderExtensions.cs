using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;

namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
    internal static class SqlCommandBuilderExtensions
    {
        public static StringBuilder AppendColumnNames(
            this StringBuilder stringBuilder,
            IReadOnlyCollection<IProperty> properties,
            bool wrapInParanthesis,
            string identifierPrefix = null,
            Func<string, string> aliaser = null)
        {
            if (wrapInParanthesis)
            {
                stringBuilder.Append("(");
            }

            return stringBuilder
                .AppendJoin(',', properties.Select(property =>
                {
                    var columnName = property.GetColumnName();
                    var column = identifierPrefix != null ? $"{identifierPrefix}.{columnName}" : columnName;
                    return aliaser != null ? $"{column} AS {aliaser(columnName)}" : column;
                }))
                .Append(wrapInParanthesis ? ") " : " ");
        }

        public static StringBuilder AppendValues<TEntity>(
            this StringBuilder stringBuilder,
            IProperty[] properties,
            IEnumerable<TEntity> entities,
            IList<object> parameters,
            bool wrapInParanthesis = false)
            where TEntity : class
        {
            stringBuilder.Append(wrapInParanthesis ? " (" : " ");

            stringBuilder.Append("VALUES");

            // built up the rows
            foreach (TEntity entity in entities)
            {
                stringBuilder.Append(" (");
                foreach (IProperty property in properties)
                {
                    // for each column value, create a placeholder, e.g. "{3}" used for the parameter
                    stringBuilder.Append("{").Append(parameters.Count).Append("},");
                    parameters.Add(property.PropertyInfo.GetValue(entity));
                }

                stringBuilder.Length--; // remove the last ","
                stringBuilder.Append("),");
            }

            stringBuilder.Length--; // remove the last ","
            return stringBuilder.Append(wrapInParanthesis ? ") " : " ");
        }

        public static StringBuilder AppendSelectFromInlineTable<TEntity>(
            this StringBuilder stringBuilder,
            IProperty[] properties,
            IEnumerable<TEntity> entities,
            IList<object> parameters,
            string tableAlias,
            bool sqliteSyntax = false)
            where TEntity : class
        {
            if (sqliteSyntax)
            {
                return stringBuilder
                .Append("SELECT * FROM (SELECT ")
                .AppendJoin(", ", Enumerable.Range(1, properties.Length).Select(columnNumber => $"[column{columnNumber}] {properties[columnNumber - 1].GetColumnName()}"))
                .Append(" FROM").AppendValues(properties, entities, parameters, wrapInParanthesis: true)
                .Append(") AS ").Append(tableAlias).Append(" ");
            }

            return stringBuilder
                .Append("SELECT * FROM ").AppendValues(properties, entities, parameters, wrapInParanthesis: true)
                .Append(" AS ").Append(tableAlias).AppendColumnNames(properties, wrapInParanthesis: true);
        }

        public static StringBuilder AppendJoinCondition(this StringBuilder stringBuilder, IKey key, string leftTableAlias = "source", string rightTableAlias = "target")
        {
            const string andOperator = " AND ";
            foreach (var keyProperty in key.Properties)
            {
                stringBuilder.Append(leftTableAlias).Append('.').Append(keyProperty.Name).Append('=').Append(rightTableAlias).Append('.').Append(keyProperty.Name)
                             .Append(andOperator);
            }

            stringBuilder.Length -= andOperator.Length;
            return stringBuilder.Append(" ");
        }

        public static StringBuilder AppendTableValuedParameter<TEntity>(
            this StringBuilder stringBuilder, 
            string userDefinedTableTypeName,
            IProperty[] properties, 
            IEnumerable<TEntity> entities, 
            IList<object> parameters)
        {
            var dataTable = new DataTable();

            // Add the columns
            foreach (IProperty property in properties)
            {
                var columnType = property.PropertyInfo.PropertyType;
                columnType = Nullable.GetUnderlyingType(columnType) ?? columnType;

                // For some reason, a datatable with an enum types throws unsupported column exceptions, so we'll convert it to an int.
                if (columnType.IsEnum)
                {
                    columnType = typeof(int);
                }

                dataTable.Columns.Add(property.GetColumnName(), columnType);
            }

            // Add the entities as rows
            foreach (TEntity entity in entities)
            {
                var row = new object[properties.Length];

                for (int i = 0; i < properties.Length; i++)
                {
                    row[i] = properties[i].PropertyInfo.GetValue(entity);
                }

                dataTable.Rows.Add(row);
            }

            var parameter = new SqlParameter
            {
                ParameterName = $"@{userDefinedTableTypeName}",
                SqlDbType = SqlDbType.Structured,
                TypeName = userDefinedTableTypeName,
                Value = dataTable,
            };
            parameters.Add(parameter);

            return stringBuilder.Append(parameter.ParameterName);
        }


    }
}
