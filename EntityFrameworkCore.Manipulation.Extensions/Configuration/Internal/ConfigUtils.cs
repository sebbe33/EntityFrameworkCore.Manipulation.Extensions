namespace EntityFrameworkCore.Manipulation.Extensions.Configuration.Internal
{
    using Microsoft.EntityFrameworkCore;
    internal static class ConfigUtils
    {
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
