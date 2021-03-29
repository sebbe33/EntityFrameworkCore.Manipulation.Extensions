namespace EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers
{
    using Microsoft.EntityFrameworkCore;
    using System;

    public class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions options)
            : base(options)
        {
            this.Database.EnsureCreated();
        }

        public DbSet<TestEntity> TestEntities { get; set; }

        public DbSet<TestEntityCompositeKey> TestEntitiesWithCompositeKey { get; set; }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<TestEntityCompositeKey>()
                .HasKey(testEntityWithCompositeKey => new { testEntityWithCompositeKey.IdPartA, testEntityWithCompositeKey.IdPartB });
        }
    }
}
