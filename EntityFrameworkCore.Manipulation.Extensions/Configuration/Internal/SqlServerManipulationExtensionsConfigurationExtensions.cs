namespace EntityFrameworkCore.Manipulation.Extensions.Configuration.Internal
{
    using System;
    using System.Collections.Generic;
    using Microsoft.EntityFrameworkCore.Metadata;

    public static class SqlServerManipulationExtensionsConfigurationExtensions
    {
        public static bool ShouldUseTableValuedParameters<TEntity>(
            this SqlServerManipulationExtensionsConfiguration confiruation,
            IReadOnlyCollection<IProperty> properties,
            IReadOnlyCollection<TEntity> entities)
        {
            if (confiruation == null)
            {
                throw new ArgumentNullException(nameof(confiruation));
            }

            return entities.Count > confiruation.UseTableValuedParametersRowTreshold || entities.Count * properties.Count > confiruation.UseTableValuedParametersParameterCountTreshold;
        }
    }
}
