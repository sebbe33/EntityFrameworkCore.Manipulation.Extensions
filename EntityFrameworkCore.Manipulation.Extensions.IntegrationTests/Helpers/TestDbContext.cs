using Microsoft.EntityFrameworkCore;


namespace EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers
{
    public class TestDbContext : DbContext
    {
        public TestDbContext(DbContextOptions options)
            : base(options)
        {
            this.Database.EnsureCreated();
        }

        public DbSet<TestEntity> TestEntities { get; set; }
    }
}
