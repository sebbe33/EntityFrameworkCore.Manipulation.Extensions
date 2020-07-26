using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;
using System;
using System.Collections.Generic;
using System.ComponentModel;

namespace EntityFrameworkCore.Manipulation.Extensions.Internal
{
    internal static class EntityUtils
    {
        public static TEntity FindEntityBasedOnKey<TEntity>(IEnumerable<TEntity> entities, IKey key, object[] keyPropertyValues)
            where TEntity : class
        {
            foreach (var entity in entities)
            {
                if (EqualBasedOnKey(entity, key, keyPropertyValues))
                {
                    return entity;
                }
            }

            return default;
        }

        public static bool EqualBasedOnKey<TEntity>(TEntity entity, IKey key, object[] keyPropertyValues)
            where TEntity : class
        {
            for (var i = 0; i < key.Properties.Count; i++)
            {
                if (key.Properties[i].PropertyInfo.GetValue(entity)?.Equals(keyPropertyValues[i]) != true)
                {
                    return false;
                }
            }

            return true;
        }

        public static TEntity EntityFromRow<TEntity>(object[] row, IProperty[] properties, int offset = 0, bool useExplicitConversion = false)
            where TEntity : class, new()
        {
            var entity = new TEntity();
            for (var i = 0; i < properties.Length; i++)
            {
                var valueConverter = properties[i].GetValueConverter();
                object rawValue = row[i + offset] is DBNull ? null : row[i + offset];

				if (useExplicitConversion && valueConverter == null)
				{
					var targetType = properties[i].PropertyInfo.PropertyType;
					var nullableUnderlyingType = Nullable.GetUnderlyingType(targetType);
					if (nullableUnderlyingType != default)
					{
						targetType = nullableUnderlyingType;
					}

					if (rawValue == null)
					{
						// Nothing to convert
					}
					else if (targetType.IsEnum)
					{
						rawValue = rawValue is string enumValue
							? Enum.Parse(targetType, enumValue, true)
							: Convert.ChangeType(rawValue, targetType.GetEnumUnderlyingType());

						if (nullableUnderlyingType != null)
						{
							rawValue = Enum.ToObject(targetType, rawValue);
						}
					}
					else if (targetType == typeof(bool) && !(rawValue is bool))
					{
						valueConverter = new BoolToZeroOneConverter<int?>();
					}
					else if (targetType == typeof(DateTime) && !(rawValue is DateTime))
					{
						valueConverter = new DateTimeToStringConverter();
					}
					else if (targetType == typeof(Guid) && !(rawValue is Guid))
					{
						valueConverter = new GuidToStringConverter();
					}
					else
					{
						try
						{
							rawValue = Convert.ChangeType(rawValue, targetType);
						}
						catch (Exception e)
						{
							throw new Exception($"Failed to convert property '{properties[i].Name}'. See inner exception for details.", e);
						}
					}

					if (rawValue != null)
					{
						properties[i].PropertyInfo.SetValue(entity, valueConverter?.ConvertFromProvider(rawValue) ?? rawValue);
					}
				}
				else
				{
					properties[i].PropertyInfo.SetValue(entity, valueConverter?.ConvertFromProvider(rawValue) ?? rawValue);
				}            }

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

		private static T CastTo<T>(object obj) => (T)obj;
    }
}
