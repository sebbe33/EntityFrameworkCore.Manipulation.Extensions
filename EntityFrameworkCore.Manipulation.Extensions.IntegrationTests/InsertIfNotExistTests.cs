namespace EntityFrameworkCore.Manipulation.Extensions.UnitTests
{
    using EntityFrameworkCore.Manipulation.Extensions;
    using EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers;
    using FluentAssertions;
    using Microsoft.Data.Sqlite;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Data.SqlClient;
    using System.Linq;
    using System.Threading.Tasks;

    [TestClass]
    public class InsertIfNotExistTests
    {
        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task InsertIfNotExistAsync_ShouldNotInsertEntities_WhenNoEntitiesGiven(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, testConfiguration: testConfiguration);

            // Validate that the DB doesn't have any entities
            (await context.TestEntities.CountAsync()).Should().Be(0);

            // Invoke the method and check that the result is the expected entities
            System.Collections.Generic.IReadOnlyCollection<TestEntity> actualInsertedEntities = await context.InsertIfNotExistAsync(context.TestEntities, new TestEntity[0]);
            actualInsertedEntities.Should().BeEmpty();

            // Then check that the actual DB contains no entities
            (await context.TestEntities.CountAsync()).Should().Be(0);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task InsertIfNotExistAsync_ShouldInsertAndReturnAllEntitiest_WhenThereAreNoExistingEntities(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] expectedEntitiesToBeInserted = new[]
            {
                new TestEntity { Id = "1", IntTestValue = 111, StringTestValue = "a", BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 15451455587546 },
                new TestEntity { Id = "3", IntTestValue = 333, StringTestValue = "a", BoolTestValue = false, DateTimeTestValue = new DateTime(15645325746541), LongTestValue = 7451524264 },
            };

            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, testConfiguration: testConfiguration);

            // Validate that the DB doesn't have any entities
            (await context.TestEntities.CountAsync()).Should().Be(0);

            // Invoke the method and check that the result is the expected entities
            System.Collections.Generic.IReadOnlyCollection<TestEntity> actualInsertedEntities = await context.InsertIfNotExistAsync(context.TestEntities, expectedEntitiesToBeInserted);
            actualInsertedEntities.Should().BeEquivalentTo(expectedEntitiesToBeInserted);

            // Then check that the actual DB contains the expected entities
            TestEntity[] actualEntities = await context.TestEntities.ToArrayAsync();
            actualEntities.Should().BeEquivalentTo(expectedEntitiesToBeInserted);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task InsertIfNotExistAsync_ShouldInsertAndReturnEntitiesWhichNotExist_WhenThereAreExistingEntities(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
            {
                new TestEntity { Id = "2", IntTestValue = 222, StringTestValue = "a" },
                new TestEntity { Id = "4", IntTestValue = 444, StringTestValue = "a" },
                new TestEntity { Id = "5", IntTestValue = 333, StringTestValue = "a" },
            };

            TestEntity[] expectedEntitiesToBeInserted = new[]
            {
                new TestEntity { Id = "1", IntTestValue = 111, StringTestValue = "a", BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 15451455587546 },
                new TestEntity { Id = "3", IntTestValue = 333, StringTestValue = "a", BoolTestValue = false, DateTimeTestValue = new DateTime(15645325746541), LongTestValue = 7451524264 },
            };

            // First, add the entities which should exist before we perform our test.
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, testConfiguration: testConfiguration);
            context.TestEntities.AddRange(existingEntities);
            await context.SaveChangesAsync();

            // Then try to insert some entities which already exists + the expectedEntitiesToBeInserted
            TestEntity[] entitiesToInsert = existingEntities.Take(2).Concat(expectedEntitiesToBeInserted).ToArray();
            System.Collections.Generic.IReadOnlyCollection<TestEntity> actualInsertedEntities = await context.InsertIfNotExistAsync(context.TestEntities, entitiesToInsert);

            // Check that the returned result is only expectedEntitiesToBeInserted, i.e. that the entities which already exist haven't been "inserted again"
            actualInsertedEntities.Should().BeEquivalentTo(expectedEntitiesToBeInserted);

            // Then check that the actual DB contains the expected entities
            TestEntity[] actualEntities = await context.TestEntities.ToArrayAsync();
            actualEntities.Should().BeEquivalentTo(existingEntities.Concat(expectedEntitiesToBeInserted));
        }

        // SQL server specific
        [TestMethod]
        public async Task InsertIfNotExistAsync_ShouldInsertDbNull_WhenObjectValueIsNull()
        {
            TestEntity[] expectedEntitiesToBeInserted = new[]
            {
                new TestEntity { Id = "1", StringTestValue = null, IntTestValue = 111, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 15451455587546 },
            };

            using TestDbContext context = await ContextFactory.GetDbContextAsync(DbProvider.SqlServer);

            // Validate that the DB doesn't have any entities
            (await context.TestEntities.CountAsync()).Should().Be(0);

            // Invoke the method and check that the result is the expected entities
            System.Collections.Generic.IReadOnlyCollection<TestEntity> actualInsertedEntities = await context.InsertIfNotExistAsync(context.TestEntities, expectedEntitiesToBeInserted);
            actualInsertedEntities.Should().BeEquivalentTo(expectedEntitiesToBeInserted);

            // Then check that the actual DB contains the expected entities
            TestEntity[] actualEntities = await context.TestEntities.ToArrayAsync();
            actualEntities.Should().BeEquivalentTo(expectedEntitiesToBeInserted);
        }
    }
}
