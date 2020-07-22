using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;

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
                object rawValue = row[i + offset];
                if (useExplicitConversion)
                {
                    var targetType = properties[i].PropertyInfo.PropertyType;
                    rawValue = Convert.ChangeType(rawValue, targetType);
                }

                properties[i].PropertyInfo.SetValue(entity, valueConverter?.ConvertFromProvider(rawValue) ?? rawValue);
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
    }
}
