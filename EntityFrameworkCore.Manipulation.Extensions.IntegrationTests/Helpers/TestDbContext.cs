using Microsoft.EntityFrameworkCore;
using System;

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

		public DbSet<TestEntityCompositeKey> TestEntitiesWithCompositeKey { get; set; }

		public DbSet<InlineTestChildEntitiyCompositeKey> TestChildEntitiesWithCompositeKey { get; set; }

		protected override void OnModelCreating(ModelBuilder modelBuilder)
		{
			modelBuilder.Entity<TestEntityCompositeKey>()
				.HasKey(testEntityWithCompositeKey => new { testEntityWithCompositeKey.IdPartA, testEntityWithCompositeKey.IdPartB });

			modelBuilder
				.Entity<InlineTestChildEntitiyCompositeKey>()
				.HasKey(testEntityWithCompositeKey => new { testEntityWithCompositeKey.IdPartA, testEntityWithCompositeKey.IdPartB });
			modelBuilder
				.Entity<InlineTestChildEntitiyCompositeKey>()
				.ToView("You forgot to use FromSql with SecurityCurrencyDevice");
		}
	}
}
