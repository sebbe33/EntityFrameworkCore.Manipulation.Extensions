namespace EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers
{
    using EntityFrameworkCore.Manipulation.Extensions.Configuration;
    using Microsoft.Data.Sqlite;
    using Microsoft.EntityFrameworkCore;
    using System;
    using System.Collections.Generic;
    using Microsoft.Data.SqlClient;
    using System.Threading.Tasks;

    public enum DbProvider
    {
        SqlServer,
        Sqlite
    }

    public static class ContextFactory
    {
        public static async Task<TestDbContext> GetDbContextAsync(DbProvider provider, IEnumerable<object> seedData = null, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            var optionsBuilder = new DbContextOptionsBuilder();

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
                    TrustServerCertificate = true,
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

                await context.Database.MigrateAsync();

                // Clear the DB
                SqlCommand command = sqlConnection.CreateCommand();
                command.CommandText = "DELETE FROM TestEntities; DELETE FROM TestEntitiesWithCompositeKey; DELETE FROM TestInterceptorEntities;";
                await command.ExecuteNonQueryAsync();
            }

            if (seedData != null)
            {
                using var seedContext = new TestDbContext(optionsBuilder.Options);
                seedContext.AddRange(seedData);
                await seedContext.SaveChangesAsync();
            }

            if (testConfiguration == TestConfiguration.SqlServerRegularTableTypes)
            {
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.UseMemoryOptimizedTableTypes = false;
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.DetaultUseTableValuedParametersParameterCountTreshold = 0;
            }
            else if (testConfiguration == TestConfiguration.SqlServerRegularTableTypesWithClusteredIndex)
            {
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.DetaultUseTableValuedParametersParameterCountTreshold = 0;
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.AddEntityConifugration<TestEntity>(new EntityConifugration { UseMemoryOptimizedTableTypes = false, TableTypeIndex = SqlServerTableTypeIndex.ClusteredIndex });
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.AddEntityConifugration<TestEntityCompositeKey>(new EntityConifugration { UseMemoryOptimizedTableTypes = false, TableTypeIndex = SqlServerTableTypeIndex.ClusteredIndex });
            }
            else if (testConfiguration == TestConfiguration.SqlServerRegularTableTypesWithNonclusteredIndex)
            {
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.DetaultUseTableValuedParametersParameterCountTreshold = 0;
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.AddEntityConifugration<TestEntity>(new EntityConifugration { UseMemoryOptimizedTableTypes = false, TableTypeIndex = SqlServerTableTypeIndex.NonClusteredIndex });
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.AddEntityConifugration<TestEntityCompositeKey>(new EntityConifugration { UseMemoryOptimizedTableTypes = false, TableTypeIndex = SqlServerTableTypeIndex.NonClusteredIndex });
            }
            else if (testConfiguration == TestConfiguration.SqlServerMemoryOptimizedTableTypes)
            {
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.UseMemoryOptimizedTableTypes = true;
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.DetaultUseTableValuedParametersParameterCountTreshold = 0;
            }
            else if (testConfiguration == TestConfiguration.SqlServerMemoryOptimizedTableTypesWithNonclusteredIndex)
            {
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.DetaultUseTableValuedParametersParameterCountTreshold = 0;
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.AddEntityConifugration<TestEntity>(new EntityConifugration { UseMemoryOptimizedTableTypes = true, TableTypeIndex = SqlServerTableTypeIndex.NonClusteredIndex });
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.AddEntityConifugration<TestEntityCompositeKey>(new EntityConifugration { UseMemoryOptimizedTableTypes = true, TableTypeIndex = SqlServerTableTypeIndex.NonClusteredIndex });
            }
            else if (testConfiguration == TestConfiguration.SqlServerOutputInto)
            {
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.AddEntityConifugration<TestEntity>(new EntityConifugration { HasTrigger = true });
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.AddEntityConifugration<TestEntityCompositeKey>(new EntityConifugration { HasTrigger = true });
            }
            else if (testConfiguration == TestConfiguration.SqlServerMergeSync)
            {
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.UseMerge = true;
            }
            else if (testConfiguration == TestConfiguration.SqlServerSimpleStatementsSync)
            {
                context.ManipulationExtensionsConfiguration.SqlServerConfiguration.UseMerge = false;
            }

            return context;
        }
    }
}
