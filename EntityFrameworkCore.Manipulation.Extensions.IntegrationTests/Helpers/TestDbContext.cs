namespace EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers
{
    using EntityFrameworkCore.Manipulation.Extensions.Configuration;
    using Microsoft.EntityFrameworkCore;

    public class TestDbContext : DbContext, IManipulationExtensionsConfiguredDbContext
    {
        public TestDbContext(DbContextOptions options)
            : base(options)
        {
            this.Database.EnsureCreated();
        }

        public DbSet<TestEntity> TestEntities { get; set; }

        public DbSet<TestEntityCompositeKey> TestEntitiesWithCompositeKey { get; set; }

        public DbSet<TestInterceptorEntity> TestInterceptorEntities { get; set; }

        public ManipulationExtensionsConfiguration ManipulationExtensionsConfiguration { get; } = new ManipulationExtensionsConfiguration();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TestEntityCompositeKey>()
                .HasKey(testEntityWithCompositeKey => new { testEntityWithCompositeKey.IdPartA, testEntityWithCompositeKey.IdPartB });
        }
    }
}
