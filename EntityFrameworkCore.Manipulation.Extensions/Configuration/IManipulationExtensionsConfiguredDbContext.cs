namespace EntityFrameworkCore.Manipulation.Extensions.Configuration
{
    /// <summary>
    /// A contract for a <see cref="DbContext"/>, used with the Manipulation Extensions lib, which contains
    /// a <see cref="ManipulationExtensionsConfiguration"/> used to configure how the lib behaves.
    /// </summary>
    public interface IManipulationExtensionsConfiguredDbContext
    {
        public ManipulationExtensionsConfiguration ManipulationExtensionsConfiguration { get; }
    }
}
