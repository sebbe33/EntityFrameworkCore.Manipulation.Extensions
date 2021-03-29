namespace EntityFrameworkCore.Manipulation.Extensions.Configuration
{
    /// <summary>
    /// Configuration for the EntityFrameworkCore.Manipulation.Extensions library.
    /// </summary>
    public class ManipulationExtensionsConfiguration
    {
        /// <summary>
        /// Configuration specific to SQL Server. All settings are only applicable when using SQL Server
        /// as the underlying provider for the Entity Framework DB Context.
        /// </summary>
        public SqlServerManipulationExtensionsConfiguration SqlServerConfiguration { get; } = new SqlServerManipulationExtensionsConfiguration();
    }
}
