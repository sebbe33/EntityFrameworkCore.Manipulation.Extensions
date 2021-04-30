namespace EntityFrameworkCore.Manipulation.Extensions.Configuration.Internal
{
    using System;
    using System.Collections.Generic;
    using EntityFrameworkCore.Manipulation.Extensions.Internal;
    using Microsoft.EntityFrameworkCore.Metadata;

    internal static class SqlServerManipulationExtensionsConfigurationExtensions
    {
        public static bool DoesEntityHaveTriggers<TEntity>(this SqlServerManipulationExtensionsConfiguration configuration) =>
            configuration.GetEntityConifugrationOrDefault<TEntity>()?.HasTrigger ?? false;

        public static int GetHashIndexBucketCount(this SqlServerManipulationExtensionsConfiguration configuration, Type entityType) =>
            configuration.GetEntityConifugrationOrDefault(entityType)?.HashBucketSizetHashIndexBucketCount ?? configuration.DefaultHashIndexBucketCount;

        public static SqlServerTableTypeIndex GetTableTypeIndex(this SqlServerManipulationExtensionsConfiguration configuration, Type entityType) =>
            configuration.GetEntityConifugrationOrDefault(entityType)?.TableTypeIndex ?? configuration.DefaultTableTypeIndex;

        public static ITableValuedParameterInterceptor GetTvpInterceptor(this SqlServerManipulationExtensionsConfiguration configuration, Type entityType) =>
            configuration.GetEntityConifugrationOrDefault(entityType)?.TableValuedParameterInterceptor ?? DefaultTableValuedParameterInterceptor.Instance;

        public static bool ShouldUseTableValuedParameters<TEntity>(
            this SqlServerManipulationExtensionsConfiguration configuration,
            IReadOnlyCollection<IProperty> properties,
            IReadOnlyCollection<TEntity> entities)
        {
            if (configuration == null)
            {
                throw new ArgumentNullException(nameof(configuration));
            }

            EntityConifugration entityConfiguration = configuration.GetEntityConifugrationOrDefault<TEntity>();
            int rowThreshold = entityConfiguration?.UseTableValuedParametersRowTreshold ?? configuration.DefaultUseTableValuedParametersRowTreshold;
            int parameterThreshold = entityConfiguration?.UseTableValuedParametersParameterCountTreshold ?? configuration.DetaultUseTableValuedParametersParameterCountTreshold;

            return entities.Count > rowThreshold || entities.Count * properties.Count > parameterThreshold;
        }

        public static bool ShouldUseMemoryOptimizedTableTypes(this SqlServerManipulationExtensionsConfiguration configuration, Type entityType) =>
            configuration.GetEntityConifugrationOrDefault(entityType)?.UseMemoryOptimizedTableTypes ?? configuration.UseMemoryOptimizedTableTypes;

        public static bool ShouldUseMerge<TEntity>(this SqlServerManipulationExtensionsConfiguration configuration) =>
            configuration.GetEntityConifugrationOrDefault<TEntity>()?.UseMerge ?? configuration.UseMerge;

        private static EntityConifugration GetEntityConifugrationOrDefault<TEntity>(this SqlServerManipulationExtensionsConfiguration configuration) =>
            configuration.GetEntityConifugrationOrDefault(typeof(TEntity));

        private static EntityConifugration GetEntityConifugrationOrDefault(this SqlServerManipulationExtensionsConfiguration configuration, Type entityType) =>
            configuration.EntityConfigurations.TryGetValue(entityType, out EntityConifugration entityConifugration) ? entityConifugration : null;
    }
}
