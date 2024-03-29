namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Metadata;
    using System;
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data;
    using Microsoft.Data.SqlClient;
    using System.Data.SqlTypes;
    using System.Linq;
    using System.Text;

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
                    string columnName = property.GetColumnName();
                    string column = identifierPrefix != null ? $"{identifierPrefix}.{columnName}" : columnName;
                    return aliaser != null ? $"{column} AS {aliaser(columnName)}" : column;
                }))
                .Append(wrapInParanthesis ? ") " : " ");
        }

        public static StringBuilder AppendValues<TEntity>(
            this StringBuilder stringBuilder,
            IProperty[] properties,
            IEnumerable<TEntity> entities,
            IList<object> parameters,
            bool wrapInParenthesis = false)
            where TEntity : class
        {
            stringBuilder.Append(wrapInParenthesis ? " (" : " ");

            stringBuilder.Append("VALUES");

            // built up the rows
            foreach (TEntity entity in entities)
            {
                stringBuilder.Append(" (");
                foreach (IProperty property in properties)
                {
                    // for each column value, create a placeholder, e.g. "{3}" used for the parameter
                    stringBuilder.Append("{").Append(parameters.Count).Append("},");

                    // then add the actual value to the list of parameters
                    parameters.Add(property.PropertyInfo.GetValue(entity));
                }

                stringBuilder.Length--; // remove the last ","
                stringBuilder.Append("),");
            }

            stringBuilder.Length--; // remove the last ","
            return stringBuilder.Append(wrapInParenthesis ? ") " : " ");
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
                .Append(" FROM").AppendValues(properties, entities, parameters, wrapInParenthesis: true)
                .Append(") AS ").Append(tableAlias).Append(" ");
            }

            return stringBuilder
                .Append("SELECT * FROM ").AppendValues(properties, entities, parameters, wrapInParenthesis: true)
                .Append(" AS ").Append(tableAlias).AppendColumnNames(properties, wrapInParanthesis: true);
        }

        public static StringBuilder AppendJoinCondition(this StringBuilder stringBuilder, IKey key, string leftTableAlias = "source", string rightTableAlias = "target")
        {
            const string andOperator = " AND ";
            foreach (IProperty keyProperty in key.Properties)
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
            IList<object> parameters) => stringBuilder.Append(CreateTableValuedParameter(userDefinedTableTypeName, properties, entities, parameters));

        public static string CreateTableValuedParameter<TEntity>(
            string userDefinedTableTypeName,
            IProperty[] properties,
            IEnumerable<TEntity> entities,
            IList<object> parameters,
            bool includeActionColumn = false)
        {
            var dataTable = new DataTable();

            // Add the columns
            foreach (IProperty property in properties)
            {
                Type columnType = property.PropertyInfo.PropertyType;
                columnType = Nullable.GetUnderlyingType(columnType) ?? columnType;

                // For some reason, a datatable with an enum types throws unsupported column exceptions, so we'll convert it to an int.
                if (columnType.IsEnum)
                {
                    columnType = typeof(int);
                }

                dataTable.Columns.Add(property.GetColumnName(), columnType);
            }

            if (includeActionColumn)
            {
                // Add the action column, which is part of every TVP type
                dataTable.Columns.Add(DatabaseFacadeExtensions.TempOutputTableActionColumn, typeof(string));
            }

            // Add the entities as rows
            foreach (TEntity entity in entities)
            {
                object[] row = new object[properties.Length];

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

            return parameter.ParameterName;
        }

        public static StringBuilder AppendOutputDeclaration(this StringBuilder stringBuilder, string userDefinedTableTypeName) =>
            stringBuilder.Append("DECLARE @tempOutput ").Append(userDefinedTableTypeName).AppendLine(";");

        public static StringBuilder AppendOutputClauseLine(
            this StringBuilder stringBuilder,
            IReadOnlyCollection<IProperty> properties,
            bool outputIntoTempTable,
            bool includeAction = false,
            string identifierPrefix = "deleted")
        {
            stringBuilder.Append("OUTPUT ");

            if (includeAction)
            {
                stringBuilder.Append("$action AS ").Append(DatabaseFacadeExtensions.TempOutputTableActionColumn).Append(", ");
            }

            stringBuilder.AppendColumnNames(properties, false, identifierPrefix);

            if (outputIntoTempTable)
            {
                stringBuilder.Append(" INTO @tempOutput");
            }

            return stringBuilder.AppendLine();
        }

        public static StringBuilder AppendOutputSelect(this StringBuilder stringBuilder,
            IReadOnlyCollection<IProperty> properties,
            bool includeAction = false)
        {
            stringBuilder.Append("SELECT ");

            if (includeAction)
            {
                stringBuilder.Append(DatabaseFacadeExtensions.TempOutputTableActionColumn).Append(", ");
            }

            return stringBuilder.AppendColumnNames(properties, wrapInParanthesis: false)
                .Append(" FROM @tempOutput ");
        }
    }
}
