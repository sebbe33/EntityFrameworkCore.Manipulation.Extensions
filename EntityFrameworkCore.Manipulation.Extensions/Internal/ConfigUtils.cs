using Microsoft.EntityFrameworkCore.Metadata;
using System.Collections.Generic;

namespace EntityFrameworkCore.Manipulation.Extensions.Internal
{
    public static class ConfigUtils
    {
        private const int TVPNumberOfRowsThreshold = 50;
        private const int TVPNumberOfParametersThreshold = 2000;

        public static bool ShouldUseTableValuedParameters<TEntity>(IReadOnlyCollection<IProperty> properties, IReadOnlyCollection<TEntity> entities)
            => entities.Count > TVPNumberOfRowsThreshold || entities.Count * properties.Count > TVPNumberOfParametersThreshold;
    }
}
