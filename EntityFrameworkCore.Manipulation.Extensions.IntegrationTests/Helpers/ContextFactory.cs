namespace EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers
{
    using Microsoft.Data.Sqlite;
    using Microsoft.EntityFrameworkCore;
    using System;
    using System.Collections.Generic;
    using System.Data.SqlClient;
    using System.Threading.Tasks;

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

            TestDbContext context = null;

            if (provider == DbProvider.Sqlite)
            {
                var sqlConnection = new SqliteConnection("Data Source=:memory:;");
                await sqlConnection.OpenAsync();
                optionsBuilder.UseSqlite(sqlConnection).EnableSensitiveDataLogging(true);

                context = new TestDbContext(optionsBuilder.Options);
            }
            else
            {
                string sqlUser = Environment.GetEnvironmentVariable("INTEGRATION_TEST_SQL_SERVER_DB_USER");
                string sqlPassword = Environment.GetEnvironmentVariable("INTEGRATION_TEST_SQL_SERVER_DB_PASSWORD");
                string sqlServer = Environment.GetEnvironmentVariable("INTEGRATION_TEST_SQL_SERVER");
                string sqldb = Environment.GetEnvironmentVariable("INTEGRATION_TEST_SQL_SERVER_DB");

                var connectionStringBuilder = new SqlConnectionStringBuilder
                {
                    DataSource = string.IsNullOrWhiteSpace(sqlServer) ? @"localhost\SQLEXPRESS" : sqlServer,
                    InitialCatalog = string.IsNullOrWhiteSpace(sqldb) ? @"entityframeworkcore-manipulation-extensions-integration-testing" : sqldb,
                };

                if (string.IsNullOrWhiteSpace(sqlUser) || string.IsNullOrWhiteSpace(sqlUser))
                {
                    connectionStringBuilder.IntegratedSecurity = true;
                }
                else
                {
                    connectionStringBuilder.UserID = sqlUser;
                    connectionStringBuilder.Password = sqlPassword;
                }

                var sqlConnection = new SqlConnection(connectionStringBuilder.ConnectionString);
                await sqlConnection.OpenAsync();
                optionsBuilder.UseSqlServer(sqlConnection).EnableSensitiveDataLogging(true);

                context = new TestDbContext(optionsBuilder.Options);

                if (!await context.Database.EnsureCreatedAsync())
                {
                    // Clear the DB
                    SqlCommand command = sqlConnection.CreateCommand();
                    command.CommandText = "DELETE FROM TestEntities; DELETE FROM TestEntitiesWithCompositeKey;";
                    await command.ExecuteNonQueryAsync();
                }
            }

            if (seedData != null)
            {
                using var seedContext = new TestDbContext(optionsBuilder.Options);
                seedContext.Database.EnsureCreated();
                seedContext.AddRange(seedData);
                await seedContext.SaveChangesAsync();
            }

            return context;
        }
    }
}
