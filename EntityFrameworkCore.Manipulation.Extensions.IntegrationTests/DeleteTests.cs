using EntityFrameworkCore.Manipulation.Extensions;
using EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers;
using FluentAssertions;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;

namespace EntityFrameworkCore.Manipulation.Extensions.UnitTests
{
    [TestClass]
    public class DeleteTests
    {
        [TestMethod]
        public async Task DeleteAsync_ShouldReturnEmptyCollection_WhenThereAreNoEntities()
        {
            using TestDbContext context = await this.GetDbContext(seedData: null); // Note: no seed data => no entities exist

			// Invoke the method and check that the result is empty
			IReadOnlyCollection<TestEntity> result = await context.DeleteAsync(context.TestEntities);

            result.Should().BeEmpty();
        }

		[TestMethod]
		public async Task DeleteAsync_ShouldReturnEmptyCollection_WhenThereAreNoMatchingEntities()
		{
			var existingEntities = new[]
			{
				new TestEntity { Id = "Should not be touched 1", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
				new TestEntity { Id = "Should not be touched 2", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
			};

			using TestDbContext context = await this.GetDbContext(existingEntities);

			// Invoke the method and check that the result is empty
			IReadOnlyCollection<TestEntity> result = await context.DeleteAsync(context.TestEntities.Where(entity => entity.Id == "Does not exist"));

			result.Should().BeEmpty();

			// Check that the DB hasn't been modified
			context.TestEntities.ToList().Should().BeEquivalentTo(existingEntities);
		}

		[TestMethod]
		public async Task DeleteAsync_ShouldReturnEntireCollection_WhenDeleteTargetIsAllEntities()
		{
			var existingEntities = new[]
			{
				new TestEntity { Id = "Should be delete 1", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
				new TestEntity { Id = "Should be delete 2", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
				new TestEntity { Id = "Should be delete 3", IntTestValue = 891564, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 894156156 },
			};

			using TestDbContext context = await this.GetDbContext(existingEntities);

			// Invoke the method and check that the result is all entities
			IReadOnlyCollection<TestEntity> result = await context.DeleteAsync(context.TestEntities);

			result.Should().BeEquivalentTo(existingEntities);

			// Check that the DB is empty
			context.TestEntities.Should().BeEmpty();
		}

		[TestMethod]
		public async Task DeleteAsync_ShouldReturnMatchingDeleteCollection_WhenDeleteTargetIsASubsetOfCollection()
		{
			var existingEntities = new[]
			{
				new TestEntity { Id = "Should be delete 1", IntTestValue = -1321, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
				new TestEntity { Id = "Should not be delete 2", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
				new TestEntity { Id = "Should be delete 3", IntTestValue = -516, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 894156156 },
			};

			using TestDbContext context = await this.GetDbContext(existingEntities);

			// Invoke the method and check that the result is the subset of entities with a negative IntTestValue 
			IReadOnlyCollection<TestEntity> result = await context.DeleteAsync(context.TestEntities.Where(entity => entity.IntTestValue < 0));

			result.Should().BeEquivalentTo(existingEntities.Where(entity => entity.IntTestValue < 0));

			// Check that the DB is empty
			context.TestEntities.Should().BeEquivalentTo(existingEntities.Where(entity => entity.IntTestValue >= 0));
		}

		private async Task<TestDbContext> GetDbContext(IEnumerable<TestEntity> seedData = null)
        {
            // var sqlConnection = new SqliteConnection("Data Source=F:\\temp\\temp.db;");
            // var sqlConnection = new SqliteConnection("Data Source=:memory:;");
            var sqlConnection = new SqlConnection("Data Source=localhost\\SQLEXPRESS;Initial Catalog=Test;Integrated Security=True;");
            await sqlConnection.OpenAsync();

            var optionsBuilder = new DbContextOptionsBuilder();

            optionsBuilder.UseSqlServer(sqlConnection).EnableSensitiveDataLogging(true);
			// optionsBuilder.UseSqlite(sqlConnection).EnableSensitiveDataLogging(true);

			var command = sqlConnection.CreateCommand();
			command.CommandText = "DELETE FROM TestEntities";
			command.ExecuteNonQuery();

			if (seedData != null)
            {
                using var seedContext = new TestDbContext(optionsBuilder.Options);
				seedContext.Database.EnsureCreated();
				seedContext.AddRange(seedData);
                await seedContext.SaveChangesAsync();
            }

            return new TestDbContext(optionsBuilder.Options);
        }
    }
}
