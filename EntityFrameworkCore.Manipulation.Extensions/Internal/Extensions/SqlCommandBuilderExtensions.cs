namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
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

    internal static class SqlCommandBuilderExtensions
    {
        private const string TempOutputTableActionColumn = "__Action";

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
            IList<object> parameters)
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

            return stringBuilder.Append(parameter.ParameterName);
        }

        public static StringBuilder AppendOutputTempTableDeclaration(
            this StringBuilder stringBuilder,
            IReadOnlyCollection<IProperty> insertedProperties,
            IReadOnlyCollection<IProperty> deletedProperties,
            bool includeAction = false)
        {
            // Create the temp table definition based on the included properties
            stringBuilder.Append("DECLARE @tempOutput TABLE(");

            // If we track both inserted and deleted, we have to use an aliaser to prefix the columns with i_ and d_ respectively to be able to tell the inserted/deleted appart.
            bool shouldUseAliaser = insertedProperties?.Count > 0 && deletedProperties?.Count > 0;

            if (includeAction) // action is used for MERGE
            {
                stringBuilder.Append(TempOutputTableActionColumn).Append(" char(6), ");
            }

            if (insertedProperties?.Count > 0)
            {
                foreach (IProperty property in insertedProperties)
                {
                    stringBuilder.Append(shouldUseAliaser ? OutputInsertedAliaser(property.GetColumnName()) : property.GetColumnName()).Append(' ').Append(property.GetColumnType()).Append(',');
                }
            }

            if (deletedProperties?.Count > 0)
            {
                foreach (IProperty property in deletedProperties)
                {
                    stringBuilder.Append(shouldUseAliaser ? OutputDeletedAliaser(property.GetColumnName()) : property.GetColumnName()).Append(' ').Append(property.GetColumnType()).Append(',');
                }
            }

            stringBuilder.Length--; // remove the last ","

            return stringBuilder.AppendLine(");");
        }

        public static StringBuilder AppendOutputClauseLine(
            this StringBuilder stringBuilder,
            IReadOnlyCollection<IProperty> insertedProperties,
            IReadOnlyCollection<IProperty> deletedProperties,
            bool outputIntoTempTable,
            bool includeAction = false)
        {
            bool shouldUseAliaser = insertedProperties?.Count > 0 && deletedProperties?.Count > 0;
            stringBuilder.Append("OUTPUT ");

            if (includeAction)
            {
                stringBuilder.Append("$action AS ").Append(TempOutputTableActionColumn).Append(", ");
            }

            // include the inserted.* columns to be outputed
            if (insertedProperties?.Count > 0)
            {
                Func<string, string> insertedAliaser = null;
                if (shouldUseAliaser)
                {
                    insertedAliaser = OutputInsertedAliaser;
                }

                stringBuilder.AppendColumnNames(insertedProperties, false, identifierPrefix: "inserted", aliaser: insertedAliaser);

                if (deletedProperties?.Count > 0)
                {
                    stringBuilder.Append(", ");
                }
            }

            // include the deleted.* columns to be outputed
            if (deletedProperties?.Count > 0)
            {
                Func<string, string> deletedAliaser = null;
                if (shouldUseAliaser)
                {
                    deletedAliaser = OutputDeletedAliaser;
                }

                stringBuilder.AppendColumnNames(deletedProperties, false, identifierPrefix: "deleted", aliaser: deletedAliaser);
            }

            if (outputIntoTempTable)
            {
                stringBuilder.Append(" INTO @tempOutput");
            }

            return stringBuilder.AppendLine();
        }

        public static StringBuilder AppendOutputSelect(this StringBuilder stringBuilder,
            IReadOnlyCollection<IProperty> insertedProperties,
            IReadOnlyCollection<IProperty> deletedProperties,
            bool includeAction = false)
        {
            bool shouldUseAliaser = insertedProperties?.Count > 0 && deletedProperties?.Count > 0;
            stringBuilder.Append("SELECT ");

            if (includeAction)
            {
                stringBuilder.Append(TempOutputTableActionColumn).Append(", ");
            }

            if (insertedProperties?.Count > 0)
            {
                foreach (IProperty property in insertedProperties)
                {
                    stringBuilder.Append(shouldUseAliaser ? OutputInsertedAliaser(property.GetColumnName()) : property.GetColumnName()).Append(',');
                }
            }

            if (deletedProperties?.Count > 0)
            {
                foreach (IProperty property in deletedProperties)
                {
                    stringBuilder.Append(shouldUseAliaser ? OutputDeletedAliaser(property.GetColumnName()) : property.GetColumnName()).Append(',');
                }
            }

            stringBuilder.Length--; // remove the last ","

            return stringBuilder.Append(" FROM @tempOutput ");
        }

        private static string OutputInsertedAliaser(string columnName) => $"i_{columnName}";

        private static string OutputDeletedAliaser(string columnName) => $"d_{columnName}";
    }
}
