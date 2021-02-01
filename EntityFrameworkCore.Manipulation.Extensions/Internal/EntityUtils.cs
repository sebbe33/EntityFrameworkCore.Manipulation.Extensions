using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace EntityFrameworkCore.Manipulation.Extensions.Internal
{
    internal static class EntityUtils
    {
		public static IEnumerable<IProperty> GetPropertiesFromExpressions<TEntity>(this IEnumerable<Expression<Func<TEntity, object>>> propertyExpressions, IEnumerable<IProperty> availableProperties)
		{
			var propertyInfos = propertyExpressions.Select(x => x.GetPropertyInfoFromExpression()).ToList();
			return propertyInfos.Select(propertyInfo => availableProperties.FirstOrDefault(property => property.PropertyInfo.Equals(propertyInfo))
				?? throw new ArgumentException($"The property {propertyInfo.Name} could not be found in the DB schema"));
		}

        public static TEntity FindEntityBasedOnKey<TEntity>(IEnumerable<TEntity> entities, IKey key, object[] keyPropertyValues, Func<object, object>[] keyValueConverters = null)
            where TEntity : class
        {
            foreach (var entity in entities)
            {
                if (EqualBasedOnKey(entity, key, keyPropertyValues, keyValueConverters))
                {
                    return entity;
                }
            }

            return default;
        }

        public static bool EqualBasedOnKey<TEntity>(TEntity entity, IKey key, object[] keyPropertyValues, Func<object, object>[] keyValueConverters = null)
            where TEntity : class
        {
            for (var i = 0; i < key.Properties.Count; i++)
            {
                object propertyValue = key.Properties[i].PropertyInfo.GetValue(entity);
				object keyValue = keyPropertyValues[i];

				if (keyValueConverters != null)
                {
					keyValue = keyValueConverters[i](keyValue);
                }

                if (propertyValue != keyPropertyValues[i] && propertyValue?.Equals(keyValue) != true)
                {
                    return false;
                }
            }

            return true;
        }

        public static TEntity EntityFromRow<TEntity>(object[] row, IProperty[] properties, int offset = 0, Func<object, object>[] propertyValueConverters = null)
            where TEntity : class, new()
        {
            var entity = new TEntity();
            for (var i = 0; i < properties.Length; i++)
            {
                var valueConverter = properties[i].GetValueConverter()?.ConvertFromProvider;
                object rawValue = row[i + offset] is DBNull ? null : row[i + offset];

                if (propertyValueConverters != null)
                {
                    valueConverter = propertyValueConverters[i];

                }

                properties[i].PropertyInfo.SetValue(entity, valueConverter != null ? valueConverter(rawValue) : rawValue);

            }

            return entity;
        }

        public static bool EqualBasedOnKey<TEntity>(TEntity entity, TEntity otherEntity, IKey key)
            where TEntity : class
        {
            foreach (IProperty keyProperty in key.Properties)
            {
                if (keyProperty.PropertyInfo.GetValue(entity)?.Equals(keyProperty.PropertyInfo.GetValue(otherEntity)) != true)
                {
                    return false;
                }
            }

            return true;
        }

        public static Func<object, object>[] GetEntityPropertiesValueConverters(IProperty[] properties)
        {
            var converters = new Func<object, object>[properties.Length];

            for (var i = 0; i < properties.Length; i++)
            {
                ValueConverter valueConverter = properties[i].GetValueConverter();

                if (valueConverter != null)
                {
                    converters[i] = valueConverter.ConvertFromProvider;
                    continue;
                }

                var targetType = properties[i].PropertyInfo.PropertyType;
                var nullableUnderlyingType = Nullable.GetUnderlyingType(targetType);
                if (nullableUnderlyingType != default)
                {
                    targetType = nullableUnderlyingType;
                }

                if (targetType.IsEnum)
                {
                    converters[i] = (rawValue) =>
                    {
                        var convertedValue = rawValue = rawValue is string enumValue
                            ? Enum.Parse(targetType, enumValue, true)
                            : Convert.ChangeType(rawValue, targetType.GetEnumUnderlyingType());

                        if (nullableUnderlyingType != null)
                        {
                            convertedValue = Enum.ToObject(targetType, rawValue);
                        }

                        return convertedValue;
                    };
                }
                else if (targetType == typeof(bool))
                {
                    converters[i] = new BoolToZeroOneConverter<int?>().ConvertFromProvider;
                }
                else if (targetType == typeof(DateTime))
                {
                    converters[i] = new DateTimeToStringConverter().ConvertFromProvider;
                }
                else if (targetType == typeof(Guid))
                {
                    converters[i] = new GuidToStringConverter().ConvertFromProvider;
                }
                else
                {
                    converters[i] = (rawValue) => Convert.ChangeType(rawValue, targetType);
                }

                // It's a nullable type and we have to account for DB nulls
                if (nullableUnderlyingType != default)
                {
                    var valueTypeConverter = converters[i];
                    converters[i] = rawValue => rawValue == null ? null : valueTypeConverter(rawValue);
                }
            }

            return converters;
        }
    }
}
