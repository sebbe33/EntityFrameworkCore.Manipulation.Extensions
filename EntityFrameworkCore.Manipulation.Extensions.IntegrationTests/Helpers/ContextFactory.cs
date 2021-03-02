using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers
{
	public enum DbProvider
	{
		SqlServer,
		Sqlite
	}

    public static class ContextFactory
    {
		public static async Task<TestDbContext> GetDbContextAsync(DbProvider provider, IEnumerable<object> seedData = null)
		{
			var optionsBuilder = new DbContextOptionsBuilder();

			ManipulationExtensionsConfiguration.AddTableValuedParameterInterceptor<TestEntityCompositeKey>(new TestTableValuedParameterInterceptor());

			if (provider == DbProvider.Sqlite)
			{
				var sqlConnection = new SqliteConnection("Data Source=:memory:;");
				await sqlConnection.OpenAsync();
				optionsBuilder.UseSqlite(sqlConnection).EnableSensitiveDataLogging(true);
			}
			else
			{
				var sqlConnection = new SqlConnection("Data Source=localhost\\SQLEXPRESS;Initial Catalog=Test;Integrated Security=True;");
				await sqlConnection.OpenAsync();
				optionsBuilder.UseSqlServer(sqlConnection).EnableSensitiveDataLogging(true);

				// Clear the DB
				var command = sqlConnection.CreateCommand();
				command.CommandText = "DELETE FROM TestEntities; DELETE FROM TestEntitiesWithCompositeKey;";
				await command.ExecuteNonQueryAsync();
			}
			
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
