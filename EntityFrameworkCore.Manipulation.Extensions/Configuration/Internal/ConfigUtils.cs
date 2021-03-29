namespace EntityFrameworkCore.Manipulation.Extensions.Configuration.Internal
{
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Metadata;
    using System.Collections.Generic;

    internal static class ConfigUtils
    {
        private const int TVPNumberOfRowsThreshold = 50;
        private const int TVPNumberOfParametersThreshold = 2000;

        public static ManipulationExtensionsConfiguration GetConfiguration(this DbContext dbContext)
        {
            // Check if the context implements 
            if (dbContext is IManipulationExtensionsConfiguredDbContext manipulationExtensionsConfiguredDbContext)
            {
                return manipulationExtensionsConfiguredDbContext.ManipulationExtensionsConfiguration;
            }

            // Return default settings
            return new ManipulationExtensionsConfiguration();
        }


    }
}
