namespace EntityFrameworkCore.Manipulation.Extensions.UnitTests
{
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

    [TestClass]
    public class SyncTests
    {
        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncAsync_ShouldInsertAndReturnEntities_WhenNoEntitiesExist(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, seedData: null, testConfiguration: testConfiguration); // Note: no seed data => no entities exist

            TestEntity[] expectedEntitiesToBeInserted = new[]
            {
                new TestEntity { Id = "2", IntTestValue = 222, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 15451455587546 },
                new TestEntity { Id = "3", IntTestValue = 333, BoolTestValue = false, DateTimeTestValue = new DateTime(15645325746541), LongTestValue = 7451524264 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncResult<TestEntity> result = await context.SyncAsync(context.TestEntities, expectedEntitiesToBeInserted);

            result.DeletedEntities.Should().BeEmpty();
            result.UpdatedEntities.Should().BeEmpty();
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesToBeInserted);

            // Then check that the right changes were persisted in the db.
            context.TestEntities.ToList().Should().BeEquivalentTo(expectedEntitiesToBeInserted);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncAsync_ShouldInsertAndReturnEntities_WhenNoMatchingEntitiesExistInTarget(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
{
                new TestEntity { Id = "1", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "4", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);


            TestEntity[] expectedEntitiesToBeInserted = new[]
            {
                new TestEntity { Id = "2", IntTestValue = 222, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 15451455587546 },
                new TestEntity { Id = "3", IntTestValue = 333, BoolTestValue = false, DateTimeTestValue = new DateTime(15645325746541), LongTestValue = 7451524264 },
            };

            // Invoke the method, excluding the existing entities, and check that the result is the expected entities
            string[] existingEntityIds = existingEntities.Select(e => e.Id).ToArray();
            string[] existingEntityIds1 = expectedEntitiesToBeInserted.Select(e => e.Id).ToArray();
            ISyncResult<TestEntity> result = await context.SyncAsync(context.TestEntities.Where(e => !existingEntityIds.Contains(e.Id)), expectedEntitiesToBeInserted);

            result.DeletedEntities.Should().BeEmpty();
            result.UpdatedEntities.Should().BeEmpty();
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesToBeInserted);

            // Then check that the right changes were persisted in the db.
            context.TestEntities.ToList().Should().BeEquivalentTo(existingEntities.Concat(expectedEntitiesToBeInserted));
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncAsync_ShouldUpdateAndReturnEntities_WhenTargetIsEntireTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
{
                new TestEntity { Id = "1", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "4", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);


            TestEntity[] expectedEntitiesToBeUpdated = new[]
            {
                new TestEntity { Id = "1", IntTestValue = 1111, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 8912308 },
                new TestEntity { Id = "4", IntTestValue = 4444, BoolTestValue = true, DateTimeTestValue = new DateTime(56154651231654), LongTestValue = 23135123 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncResult<TestEntity> result = await context.SyncAsync(context.TestEntities, expectedEntitiesToBeUpdated);

            result.DeletedEntities.Should().BeEmpty();
            result.InsertedEntities.Should().BeEmpty();
            result.UpdatedEntities.Should().BeEquivalentTo(new[] { (existingEntities[0], expectedEntitiesToBeUpdated[0]), (existingEntities[1], expectedEntitiesToBeUpdated[1]) });

            // Then check that the right changes were persisted in the db.
            context.TestEntities.ToList().Should().BeEquivalentTo(expectedEntitiesToBeUpdated);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncAsync_ShouldUpdateAndReturnMatchingTargetEntities_WhenTargetIsSubsetOfTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
{
                new TestEntity { Id = "1", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "4", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "ExcludedEntity1", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "ExcludedEntity2", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntity[] expectedEntitiesToBeUpdated = new[]
            {
                new TestEntity { Id = "1", IntTestValue = 1111, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 8912308 },
                new TestEntity { Id = "4", IntTestValue = 4444, BoolTestValue = true, DateTimeTestValue = new DateTime(56154651231654), LongTestValue = 23135123 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncResult<TestEntity> result = await context.SyncAsync(context.TestEntities.Where(e => !e.Id.StartsWith("Excluded")), expectedEntitiesToBeUpdated);

            result.DeletedEntities.Should().BeEmpty();
            result.InsertedEntities.Should().BeEmpty();
            result.UpdatedEntities.Should().BeEquivalentTo(new[] { (existingEntities[0], expectedEntitiesToBeUpdated[0]), (existingEntities[1], expectedEntitiesToBeUpdated[1]) });

            // Then check that the right changes were persisted in the db.
            context.TestEntities.ToList().Should().BeEquivalentTo(expectedEntitiesToBeUpdated.Concat(existingEntities.Skip(2)));
        }


        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        [Ignore("Need to fix this case")]
        public async Task SyncAsync_ShouldDeleteAndReturnEntireSource_WhenSourceIsEmpty(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
{
                new TestEntity { Id = "1", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "4", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            // Invoke the method and check that the result is the expected entities
            ISyncResult<TestEntity> result = await context.SyncAsync(context.TestEntities, new TestEntity[0]);

            result.DeletedEntities.Should().BeEquivalentTo(existingEntities);
            result.InsertedEntities.Should().BeEmpty();
            result.UpdatedEntities.Should().BeEmpty();

            // Then check that the db is empty
            context.TestEntities.ToList().Should().BeEmpty();
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncAsync_ShouldDeleteAndUpdateAndReturnEntities_WhenTargetIsEntireTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
{
                new TestEntity { Id = "TO BE DELETED", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "TO BE UPDATED", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntity[] expectedEntitiesAfterSync = new[]
            {
                new TestEntity { Id = "TO BE UPDATED", IntTestValue = 4444, BoolTestValue = true, DateTimeTestValue = new DateTime(56154651231654), LongTestValue = 23135123 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncResult<TestEntity> result = await context.SyncAsync(context.TestEntities, expectedEntitiesAfterSync);

            result.DeletedEntities.Should().BeEquivalentTo(existingEntities.Take(1));
            result.InsertedEntities.Should().BeEmpty();
            result.UpdatedEntities.Should().BeEquivalentTo(new[] { (existingEntities[1], expectedEntitiesAfterSync[0]) });

            // Then check that the the changes were synced to the db
            context.TestEntities.ToList().Should().BeEquivalentTo(expectedEntitiesAfterSync);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncAsync_ShouldDeleteAndUpdateAndReturnMatchingTargetEntities_WhenTargetIsSubsetOfTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
            {
                new TestEntity { Id = "TO BE DELETED", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "ExcludedEntity1", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "TO BE UPDATED", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "ExcludedEntity2", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntity[] expectedEntitiesInTargetAfterSync = new[]
            {
                new TestEntity { Id = "TO BE UPDATED", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncResult<TestEntity> result = await context.SyncAsync(context.TestEntities.Where(e => !e.Id.StartsWith("Excluded")), expectedEntitiesInTargetAfterSync);

            result.DeletedEntities.Should().BeEquivalentTo(existingEntities.Take(1));
            result.InsertedEntities.Should().BeEmpty();
            result.UpdatedEntities.Should().BeEquivalentTo(new[] { (existingEntities[2], expectedEntitiesInTargetAfterSync[0]) });

            // Then check that the the changes were synced to the db
            context.TestEntities.ToList().Should().BeEquivalentTo(new[] { existingEntities[1], expectedEntitiesInTargetAfterSync[0], existingEntities[3] });
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncAsync_ShouldDeleteAndInsertAndReturnMatchingTargetEntities_WhenTargetIsEntireTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
            {
                new TestEntity { Id = "TO BE DELETED", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165, NullableGuidValue = Guid.NewGuid() },
                new TestEntity { Id = "TO BE DELETED2", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntity[] expectedEntitiesInTargetAfterSync = new[]
            {
                new TestEntity { Id = "TO BE INSERTED 1", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123 },
                new TestEntity { Id = "TO BE INSERTED 2", IntTestValue = 87132, BoolTestValue = false, DateTimeTestValue = new DateTime(81846213546), LongTestValue = 87421354 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncResult<TestEntity> result = await context.SyncAsync(context.TestEntities, expectedEntitiesInTargetAfterSync);

            result.DeletedEntities.Should().BeEquivalentTo(existingEntities);
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync);
            result.UpdatedEntities.Should().BeEmpty();

            // Then check that the the changes were synced to the db
            context.TestEntities.ToList().Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncAsync_ShouldDeleteAndInsertAndReturnMatchingTargetEntities_WhenTargetIsSubsetOfTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
            {
                new TestEntity { Id = "TO BE DELETED", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "ExcludedEntity1", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "ExcludedEntity2", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "TO BE DELETED2", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntity[] expectedEntitiesInTargetAfterSync = new[]
            {
                new TestEntity { Id = "TO BE INSERTED 1", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123 },
                new TestEntity { Id = "TO BE INSERTED 2", IntTestValue = 87132, BoolTestValue = false, DateTimeTestValue = new DateTime(81846213546), LongTestValue = 87421354 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncResult<TestEntity> result = await context.SyncAsync(context.TestEntities.Where(e => !e.Id.StartsWith("Excluded")), expectedEntitiesInTargetAfterSync);

            result.DeletedEntities.Should().BeEquivalentTo(new[] { existingEntities[0], existingEntities[3] });
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync);
            result.UpdatedEntities.Should().BeEmpty();

            // Then check that the the changes were synced to the db
            context.TestEntities.ToList().Should().BeEquivalentTo(new[] { existingEntities[1], existingEntities[2], expectedEntitiesInTargetAfterSync[0], expectedEntitiesInTargetAfterSync[1] });
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncAsync_ShouldInsertAndUpdateAndReturnMatchingTargetEntities_WhenTargetIsEntireTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
            {
                new TestEntity { Id = "TO BE UPDATED", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "TO BE UPDATED2", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntity[] expectedEntitiesInTargetAfterSync = new[]
            {
                new TestEntity { Id = "TO BE INSERTED 1", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123 },
                new TestEntity { Id = "TO BE INSERTED 2", IntTestValue = 87132, BoolTestValue = false, DateTimeTestValue = new DateTime(81846213546), LongTestValue = 87421354 },
                new TestEntity { Id = "TO BE UPDATED", IntTestValue = 51564, BoolTestValue = false, DateTimeTestValue = new DateTime(95131540), LongTestValue = 6513213654 },
                new TestEntity { Id = "TO BE UPDATED2", IntTestValue = 864520, BoolTestValue = true, DateTimeTestValue = new DateTime(564584123453), LongTestValue = 56121 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncResult<TestEntity> result = await context.SyncAsync(context.TestEntities, expectedEntitiesInTargetAfterSync);

            result.DeletedEntities.Should().BeEmpty();
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync.Take(2));
            result.UpdatedEntities.Should().BeEquivalentTo(new[] { (existingEntities[0], expectedEntitiesInTargetAfterSync[2]), (existingEntities[1], expectedEntitiesInTargetAfterSync[3]) });

            // Then check that the the changes were synced to the db
            context.TestEntities.ToList().Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncAsync_ShouldInsertAndUpdateAndReturnMatchingTargetEntities_WhenTargetIsSubsetOfTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
            {
                new TestEntity { Id = "ExcludedEntity2", IntTestValue = 5645, BoolTestValue = true, DateTimeTestValue = new DateTime(884651321), LongTestValue = 32165413 },
                new TestEntity { Id = "TO BE UPDATED", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "ExcludedEntity1", IntTestValue = 555, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "TO BE UPDATED2", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntity[] expectedEntitiesInTargetAfterSync = new[]
            {
                new TestEntity { Id = "TO BE INSERTED 1", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123 },
                new TestEntity { Id = "TO BE INSERTED 2", IntTestValue = 87132, BoolTestValue = false, DateTimeTestValue = new DateTime(81846213546), LongTestValue = 87421354 },
                new TestEntity { Id = "TO BE UPDATED", IntTestValue = 51564, BoolTestValue = false, DateTimeTestValue = new DateTime(95131540), LongTestValue = 6513213654 },
                new TestEntity { Id = "TO BE UPDATED2", IntTestValue = 864520, BoolTestValue = true, DateTimeTestValue = new DateTime(564584123453), LongTestValue = 56121 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncResult<TestEntity> result = await context.SyncAsync(context.TestEntities.Where(e => !e.Id.StartsWith("Excluded")), expectedEntitiesInTargetAfterSync);

            result.DeletedEntities.Should().BeEmpty();
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync.Take(2));
            result.UpdatedEntities.Should().BeEquivalentTo(new[] { (existingEntities[1], expectedEntitiesInTargetAfterSync[2]), (existingEntities[3], expectedEntitiesInTargetAfterSync[3]) });

            // Then check that the the changes were synced to the db
            context.TestEntities.ToList().Should().BeEquivalentTo(new[] { existingEntities[0], existingEntities[2] }.Concat(expectedEntitiesInTargetAfterSync));
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncAsync_ShouldInsertAndUpdateAndDeleteAndReturnMatchingTargetEntities_WhenTargetIsEntireTablee(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
            {
                new TestEntity { Id = "TO BE DELETED", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "TO BE UPDATED", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "TO BE UPDATED2", IntTestValue = 555, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "TO BE DELETED2", IntTestValue = 6516, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntity[] expectedEntitiesInTargetAfterSync = new[]
            {
                new TestEntity { Id = "TO BE INSERTED 1", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123 },
                new TestEntity { Id = "TO BE INSERTED 2", IntTestValue = 87132, BoolTestValue = false, DateTimeTestValue = new DateTime(81846213546), LongTestValue = 87421354 },
                new TestEntity { Id = "TO BE UPDATED", IntTestValue = 51564, BoolTestValue = false, DateTimeTestValue = new DateTime(95131540), LongTestValue = 6513213654 },
                new TestEntity { Id = "TO BE UPDATED2", IntTestValue = 864520, BoolTestValue = true, DateTimeTestValue = new DateTime(564584123453), LongTestValue = 56121 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncResult<TestEntity> result = await context.SyncAsync(context.TestEntities.Where(e => !e.Id.StartsWith("Excluded")), expectedEntitiesInTargetAfterSync);

            result.DeletedEntities.Should().BeEquivalentTo(new[] { existingEntities[0], existingEntities[3] });
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync.Take(2));
            result.UpdatedEntities.Should().BeEquivalentTo(new[] { (existingEntities[1], expectedEntitiesInTargetAfterSync[2]), (existingEntities[2], expectedEntitiesInTargetAfterSync[3]) });

            // Then check that the the changes were synced to the db
            context.TestEntities.ToList().Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncAsync_ShouldInsertAndUpdateAndDeleteAndReturnMatchingTargetEntities_WhenTargetIsSubsetOfTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
            {
                new TestEntity { Id = "TO BE DELETED", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "ExcludedEntity2", IntTestValue = 5645, BoolTestValue = true, DateTimeTestValue = new DateTime(884651321), LongTestValue = 32165413 },
                new TestEntity { Id = "TO BE UPDATED", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "ExcludedEntity1", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "TO BE UPDATED2", IntTestValue = 555, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "TO BE DELETED2", IntTestValue = 6516, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntity[] expectedEntitiesInTargetAfterSync = new[]
            {
                new TestEntity { Id = "TO BE INSERTED 1", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123 },
                new TestEntity { Id = "TO BE INSERTED 2", IntTestValue = 87132, BoolTestValue = false, DateTimeTestValue = new DateTime(81846213546), LongTestValue = 87421354 },
                new TestEntity { Id = "TO BE UPDATED", IntTestValue = 51564, BoolTestValue = false, DateTimeTestValue = new DateTime(95131540), LongTestValue = 6513213654 },
                new TestEntity { Id = "TO BE UPDATED2", IntTestValue = 864520, BoolTestValue = true, DateTimeTestValue = new DateTime(564584123453), LongTestValue = 56121 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncResult<TestEntity> result = await context.SyncAsync(context.TestEntities.Where(e => !e.Id.StartsWith("Excluded")), expectedEntitiesInTargetAfterSync);

            result.DeletedEntities.Should().BeEquivalentTo(new[] { existingEntities[0], existingEntities[5] });
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync.Take(2));
            result.UpdatedEntities.Should().BeEquivalentTo(new[] { (existingEntities[2], expectedEntitiesInTargetAfterSync[2]), (existingEntities[4], expectedEntitiesInTargetAfterSync[3]) });

            // Then check that the the changes were synced to the db
            context.TestEntities.ToList().Should().BeEquivalentTo(new[] { existingEntities[1], existingEntities[3] }.Concat(expectedEntitiesInTargetAfterSync));
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncWithoutUpdateAsync_ShouldInsertAndReturnEntities_WhenNoEntitiesExist(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, seedData: null, testConfiguration: testConfiguration); // Note: no seed data => no entities exist

            TestEntity[] expectedEntitiesToBeInserted = new[]
            {
                new TestEntity { Id = "2", IntTestValue = 222, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 15451455587546 },
                new TestEntity { Id = "3", IntTestValue = 333, BoolTestValue = false, DateTimeTestValue = new DateTime(15645325746541), LongTestValue = 7451524264 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncWithoutUpdateResult<TestEntity> result = await context.SyncWithoutUpdateAsync(context.TestEntities, expectedEntitiesToBeInserted);

            result.DeletedEntities.Should().BeEmpty();
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesToBeInserted);

            // Then check that the right changes were persisted in the db.
            context.TestEntities.ToList().Should().BeEquivalentTo(expectedEntitiesToBeInserted);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncWithoutUpdateAsync_ShouldInsertAndReturnEntities_WhenNoMatchingEntitiesExistInTarget(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
{
                new TestEntity { Id = "1", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "4", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);


            TestEntity[] expectedEntitiesToBeInserted = new[]
            {
                new TestEntity { Id = "2", IntTestValue = 222, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 15451455587546 },
                new TestEntity { Id = "3", IntTestValue = 333, BoolTestValue = false, DateTimeTestValue = new DateTime(15645325746541), LongTestValue = 7451524264 },
            };

            // Invoke the method, excluding the existing entities, and check that the result is the expected entities
            string[] existingEntityIds = existingEntities.Select(e => e.Id).ToArray();
            ISyncWithoutUpdateResult<TestEntity> result = await context.SyncWithoutUpdateAsync(context.TestEntities.Where(e => !existingEntityIds.Contains(e.Id)), expectedEntitiesToBeInserted);

            result.DeletedEntities.Should().BeEmpty();
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesToBeInserted);

            // Then check that the right changes were persisted in the db.
            context.TestEntities.ToList().Should().BeEquivalentTo(existingEntities.Concat(expectedEntitiesToBeInserted));
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncWithoutUpdateAsync_ShouldDeleteAndInsertAndReturnMatchingTargetEntities_WhenTargetIsEntireTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            // Note: SyncWithoutUpdate ignores matched items, i.e. it doesn't update them. As such, we expected them to stay intact
            TestEntity[] existingEntities = new[]
            {
                new TestEntity { Id = "TO BE DELETED", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "MATCH 1 - Should Be Ignored", IntTestValue = 222, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 5641324 },
                new TestEntity { Id = "TO BE DELETED2", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "MATCH 2 - Should Be Ignored", IntTestValue = 333, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 1354126 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntity[] expectedEntitiesInTargetAfterSync = new[]
            {
                new TestEntity { Id = "TO BE INSERTED 1", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123 },
                new TestEntity { Id = "TO BE INSERTED 2", IntTestValue = 87132, BoolTestValue = false, DateTimeTestValue = new DateTime(81846213546), LongTestValue = 87421354 },
                new TestEntity { Id = "MATCH 1 - Should Be Ignored", IntTestValue = 9932165, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 5641324 },
                new TestEntity { Id = "MATCH 2 - Should Be Ignored", IntTestValue = 985613, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 5641324 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncWithoutUpdateResult<TestEntity> result = await context.SyncWithoutUpdateAsync(context.TestEntities, expectedEntitiesInTargetAfterSync);

            result.DeletedEntities.Should().BeEquivalentTo(existingEntities.Where(e => e.Id.StartsWith("TO BE DELETED")));
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync.Where(e => e.Id.StartsWith("TO BE INSERTED")));

            // Then check that the the changes were synced to the db
            context.TestEntities.ToList().Should().BeEquivalentTo(
                existingEntities.Where(e => e.Id.StartsWith("MATCH"))
                .Concat(expectedEntitiesInTargetAfterSync.Where(e => e.Id.StartsWith("TO BE INSERTED"))));
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task SyncWithoutUpdateAsync_ShouldDeleteAndInsertAndReturnMatchingTargetEntities_WhenTargetIsSubsetOfTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            // Note: SyncWithoutUpdate ignores matched items, i.e. it doesn't update them. As such, we expected them to stay intact
            TestEntity[] existingEntities = new[]
            {
                new TestEntity { Id = "TO BE DELETED", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "MATCH 1 - Should Be Ignored", IntTestValue = 222, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 5641324 },
                new TestEntity { Id = "ExcludedEntity1", IntTestValue = 5645, BoolTestValue = true, DateTimeTestValue = new DateTime(884651321), LongTestValue = 32165413 },
                new TestEntity { Id = "ExcludedEntity2", IntTestValue = 165, BoolTestValue = true, DateTimeTestValue = new DateTime(998416312), LongTestValue = 135431 },
                new TestEntity { Id = "TO BE DELETED2", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "MATCH 2 - Should Be Ignored", IntTestValue = 333, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 1354126 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntity[] expectedEntitiesInTargetAfterSync = new[]
            {
                new TestEntity { Id = "TO BE INSERTED 1", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123 },
                new TestEntity { Id = "TO BE INSERTED 2", IntTestValue = 87132, BoolTestValue = false, DateTimeTestValue = new DateTime(81846213546), LongTestValue = 87421354 },
                new TestEntity { Id = "MATCH 1 - Should Be Ignored", IntTestValue = 9932165, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 5641324 },
                new TestEntity { Id = "MATCH 2 - Should Be Ignored", IntTestValue = 985613, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 5641324 },
            };

            // Invoke the method and check that the result is the expected entities
            ISyncWithoutUpdateResult<TestEntity> result = await context.SyncWithoutUpdateAsync(context.TestEntities.Where(e => !e.Id.StartsWith("Excluded")), expectedEntitiesInTargetAfterSync);

            result.DeletedEntities.Should().BeEquivalentTo(existingEntities.Where(e => e.Id.StartsWith("TO BE DELETED")));
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync.Where(e => e.Id.StartsWith("TO BE INSERTED")));

            // Then check that the the changes were synced to the db
            context.TestEntities.ToList().Should().BeEquivalentTo(
                existingEntities.Where(e => e.Id.StartsWith("MATCH") || e.Id.StartsWith("Excluded"))
                .Concat(expectedEntitiesInTargetAfterSync.Where(e => e.Id.StartsWith("TO BE INSERTED"))));
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task UpsertAsync_ShouldInsertAndReturnEntities_WhenNoEntitiesExist(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, seedData: null, testConfiguration: testConfiguration); // Note: no seed data => no entities exist

            TestEntity[] expectedEntitiesToBeInserted = new[]
            {
                new TestEntity { Id = "2", IntTestValue = 222, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 15451455587546 },
                new TestEntity { Id = "3", IntTestValue = 333, BoolTestValue = false, DateTimeTestValue = new DateTime(15645325746541), LongTestValue = 7451524264 },
            };

            // Invoke the method and check that the result is the expected entities
            IUpsertResult<TestEntity> result = await context.UpsertAsync(expectedEntitiesToBeInserted);

            result.UpdatedEntities.Should().BeEmpty();
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesToBeInserted);

            // Then check that the right changes were persisted in the db.
            context.TestEntities.ToList().Should().BeEquivalentTo(expectedEntitiesToBeInserted);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task UpsertAsync_ShouldInsertAndReturnEntities_WhenNoMatchingEntitiesExistInTarget(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
{
                new TestEntity { Id = "1", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "4", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);


            TestEntity[] expectedEntitiesToBeInserted = new[]
            {
                new TestEntity { Id = "2", IntTestValue = 222, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 15451455587546 },
                new TestEntity { Id = "3", IntTestValue = 333, BoolTestValue = false, DateTimeTestValue = new DateTime(15645325746541), LongTestValue = 7451524264 },
            };


            IUpsertResult<TestEntity> result = await context.UpsertAsync(expectedEntitiesToBeInserted);

            result.UpdatedEntities.Should().BeEmpty();
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesToBeInserted);

            // Then check that the right changes were persisted in the db.
            context.TestEntities.ToList().Should().BeEquivalentTo(existingEntities.Concat(expectedEntitiesToBeInserted));
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task UpsertAsync_ShouldUpdateAndReturnEntities_WhenAllEntitiesMatchInTarget(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntity[] existingEntities = new[]
            {
                new TestEntity { Id = "1", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "Should Be Peristed 1", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                new TestEntity { Id = "4", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "Should Be Peristed 2", IntTestValue = 9932165, BoolTestValue = false, DateTimeTestValue = new DateTime(654165132), LongTestValue = 5641324 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);


            TestEntity[] expectedEntitiesToBeUpdated = new[]
            {
                new TestEntity { Id = "1", IntTestValue = 1111, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65132465 },
                new TestEntity { Id = "4", IntTestValue = 4444, BoolTestValue = true, DateTimeTestValue = new DateTime(9841631654), LongTestValue = 2651354 },
            };


            IUpsertResult<TestEntity> result = await context.UpsertAsync(expectedEntitiesToBeUpdated);

            result.UpdatedEntities.Should().BeEquivalentTo(new[] { (existingEntities[0], expectedEntitiesToBeUpdated[0]), (existingEntities[2], expectedEntitiesToBeUpdated[1]) });
            result.InsertedEntities.Should().BeEmpty();

            // Then check that the the changes were synced to the db. The resulting table should not touch entries which did not match
            context.TestEntities.ToList().Should().BeEquivalentTo(
                existingEntities.Where(e => e.Id.StartsWith("Should Be Peristed"))
                .Concat(expectedEntitiesToBeUpdated));
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task UpsertAsync_ShouldInsertAndUpdateAndReturnMatchingTargetEntities_WhenTargetIsEntireTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            // Note: SyncWithoutUpdate ignores matched items, i.e. it doesn't update them. As such, we expected them to stay intact
            TestEntity[] existingEntities = new[]
            {
                new TestEntity { Id = "Should Be Peristed 1", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                new TestEntity { Id = "TO BE UPDATED1", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntity { Id = "TO BE UPDATED2", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521 },
                new TestEntity { Id = "Should Be Peristed 2", IntTestValue = 9932165, BoolTestValue = false, DateTimeTestValue = new DateTime(654165132), LongTestValue = 5641324 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntity[] expectedEntitiesInTargetAfterSync = new[]
            {
                new TestEntity { Id = "TO BE INSERTED 1", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123 },
                new TestEntity { Id = "TO BE INSERTED 2", IntTestValue = 87132, BoolTestValue = false, DateTimeTestValue = new DateTime(81846213546), LongTestValue = 87421354 },
                new TestEntity { Id = "TO BE UPDATED1", IntTestValue = 3216, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 89132453 },
                new TestEntity { Id = "TO BE UPDATED2", IntTestValue = 651654, BoolTestValue = false, DateTimeTestValue = new DateTime(6541231654132163), LongTestValue = 21324351 },
            };

            // Invoke the method and check that the result is the expected entities
            IUpsertResult<TestEntity> result = await context.UpsertAsync(expectedEntitiesInTargetAfterSync);

            result.UpdatedEntities.Should().BeEquivalentTo(new[] { (existingEntities[1], expectedEntitiesInTargetAfterSync[2]), (existingEntities[2], expectedEntitiesInTargetAfterSync[3]) });
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync.Where(e => e.Id.StartsWith("TO BE INSERTED")));

            // Then check that the the changes were synced to the db. The resulting table should not touch entries which did not match
            context.TestEntities.ToList().Should().BeEquivalentTo(
                existingEntities.Where(e => e.Id.StartsWith("Should Be Peristed"))
                .Concat(expectedEntitiesInTargetAfterSync));
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task UpsertAsync_ShouldInsertAndUpdateIncludedPropertiesAndReturnMatchingTargetEntities_WhenTargetIsEntireTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            // Note: SyncWithoutUpdate ignores matched items, i.e. it doesn't update them. As such, we expected them to stay intact
            TestEntityCompositeKey[] existingEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Existing 1", IdPartB = "B", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                new TestEntityCompositeKey { IdPartA = "TO BE UPDATED1", IdPartB = "B", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165, StringTestValue = "Original 1" },
                new TestEntityCompositeKey { IdPartA = "TO BE UPDATED2", IdPartB = "B", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521, StringTestValue = "Original 2" },
                new TestEntityCompositeKey { IdPartA = "Existing 2", IdPartB = "B", IntTestValue = 9932165, BoolTestValue = false, DateTimeTestValue = new DateTime(654165132), LongTestValue = 5641324 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntityCompositeKey[] entitiesToUpsert = new[]
            {
                new TestEntityCompositeKey { IdPartA = "TO BE INSERTED 1", IdPartB = "B", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123, StringTestValue = "Well well" },
                new TestEntityCompositeKey { IdPartA = "TO BE INSERTED 2", IdPartB = "B", IntTestValue = 87132, BoolTestValue = false, DateTimeTestValue = new DateTime(81846213546), LongTestValue = 87421354, StringTestValue = "Well" },
                new TestEntityCompositeKey { IdPartA = "TO BE UPDATED1", IdPartB = "B", IntTestValue = 3216, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 89132453, StringTestValue = "Not included", GuidValue = Guid.NewGuid() },
                new TestEntityCompositeKey { IdPartA = "TO BE UPDATED2", IdPartB = "B", IntTestValue = 651654, BoolTestValue = false, DateTimeTestValue = new DateTime(6541231654132163), LongTestValue = 21324351, StringTestValue = "Not included", GuidValue = Guid.NewGuid() },
            };

            TestEntityCompositeKey[] expectedEntitiesInTargetAfterSync = new[]
            {
				// Two entities should not be touched
				existingEntities[0],
                existingEntities[3],

				// Inserted. Note: none of the nullable fields are present and should result in default values
				new TestEntityCompositeKey { IdPartA = "TO BE INSERTED 1", IdPartB = "B", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123, },
                new TestEntityCompositeKey { IdPartA = "TO BE INSERTED 2", IdPartB = "B", IntTestValue = 87132, BoolTestValue = false, DateTimeTestValue = new DateTime(81846213546), LongTestValue = 87421354, },

				// Updated. Only the fields in the inclusion builder should be updated. The rest should have their original value
				new TestEntityCompositeKey { IdPartA = "TO BE UPDATED1", IdPartB = "B", IntTestValue = entitiesToUpsert[2].IntTestValue, BoolTestValue = existingEntities[1].BoolTestValue, DateTimeTestValue = entitiesToUpsert[2].DateTimeTestValue, LongTestValue = entitiesToUpsert[2].LongTestValue, StringTestValue = existingEntities[1].StringTestValue },
                new TestEntityCompositeKey { IdPartA = "TO BE UPDATED2", IdPartB = "B", IntTestValue = entitiesToUpsert[3].IntTestValue, BoolTestValue = existingEntities[2].BoolTestValue, DateTimeTestValue = entitiesToUpsert[3].DateTimeTestValue, LongTestValue = entitiesToUpsert[3].LongTestValue, StringTestValue = existingEntities[2].StringTestValue },
            };

            // Include all fields which are non-nullable
            InclusionBuilder<TestEntityCompositeKey> insertInclusionBuilder = new InclusionBuilder<TestEntityCompositeKey>()
                .Include(x => x.BoolTestValue, x => x.DateTimeTestValue, x => x.EnumValue, x => x.GuidValue, x => x.IntTestValue, x => x.LongTestValue);

            InclusionBuilder<TestEntityCompositeKey> updateInclusionBuilder = new InclusionBuilder<TestEntityCompositeKey>()
                .Include(x => x.IntTestValue, x => x.DateTimeTestValue)
                .Include(nameof(TestEntity.LongTestValue));

            // Invoke the method and check that the result is the expected entities
            IUpsertResult<TestEntityCompositeKey> result = await context.UpsertAsync(
                entitiesToUpsert,
                insertClusivityBuilder: insertInclusionBuilder,
                updateClusivityBuilder: updateInclusionBuilder);

            result.UpdatedEntities.Should().BeEquivalentTo(new[] { (existingEntities[1], expectedEntitiesInTargetAfterSync[4]), (existingEntities[2], expectedEntitiesInTargetAfterSync[5]) });
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync.Where(entity => entity.IdPartA.StartsWith("TO BE INSERTED")));

            // Then check that the the changes were synced to the db. The resulting table should not touch entries which did not match, nor should it contain non-included fields
            context.TestEntitiesWithCompositeKey.ToList().Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerOutputInto)]
        public async Task UpsertAsync_ShouldInsertAndUpdateNonExcludedPropertiesAndReturnMatchingTargetEntities_WhenTargetIsEntireTable(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            // Note: SyncWithoutUpdate ignores matched items, i.e. it doesn't update them. As such, we expected them to stay intact
            TestEntityCompositeKey[] existingEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Existing 1", IdPartB = "B", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                new TestEntityCompositeKey { IdPartA = "TO BE UPDATED1", IdPartB = "B", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165, StringTestValue = "Original 1" },
                new TestEntityCompositeKey { IdPartA = "TO BE UPDATED2", IdPartB = "B", IntTestValue = 444, BoolTestValue = false, DateTimeTestValue = new DateTime(55644547416541), LongTestValue = 89413543521, StringTestValue = "Original 2" },
                new TestEntityCompositeKey { IdPartA = "Existing 2", IdPartB = "B", IntTestValue = 9932165, BoolTestValue = false, DateTimeTestValue = new DateTime(654165132), LongTestValue = 5641324 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, existingEntities, testConfiguration: testConfiguration);

            TestEntityCompositeKey[] entitiesToUpsert = new[]
            {
                new TestEntityCompositeKey { IdPartA = "TO BE INSERTED 1", IdPartB = "B", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123, StringTestValue = "Well well" },
                new TestEntityCompositeKey { IdPartA = "TO BE INSERTED 2", IdPartB = "B", IntTestValue = 87132, BoolTestValue = false, DateTimeTestValue = new DateTime(81846213546), LongTestValue = 87421354, StringTestValue = "Well" },
                new TestEntityCompositeKey { IdPartA = "TO BE UPDATED1", IdPartB = "B", IntTestValue = 3216, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 89132453, StringTestValue = "Not included", GuidValue = Guid.NewGuid() },
                new TestEntityCompositeKey { IdPartA = "TO BE UPDATED2", IdPartB = "B", IntTestValue = 651654, BoolTestValue = false, DateTimeTestValue = new DateTime(6541231654132163), LongTestValue = 21324351, StringTestValue = "Not included", GuidValue = Guid.NewGuid() },
            };

            TestEntityCompositeKey[] expectedEntitiesInTargetAfterSync = new[]
            {
				// Two entities should not be touched
				existingEntities[0],
                existingEntities[3],

				// Inserted. Note: none of the nullable fields are present and should result in default values
				new TestEntityCompositeKey { IdPartA = "TO BE INSERTED 1", IdPartB = "B", IntTestValue = 84106, BoolTestValue = true, DateTimeTestValue = new DateTime(846213546), LongTestValue = 32425123, },
                new TestEntityCompositeKey { IdPartA = "TO BE INSERTED 2", IdPartB = "B", IntTestValue = 87132, BoolTestValue = false, DateTimeTestValue = new DateTime(81846213546), LongTestValue = 87421354, },

				// Updated. Only the fields in the inclusion builder should be updated. The rest should have their original value
				new TestEntityCompositeKey { IdPartA = "TO BE UPDATED1", IdPartB = "B", IntTestValue = entitiesToUpsert[2].IntTestValue, BoolTestValue = existingEntities[1].BoolTestValue, DateTimeTestValue = entitiesToUpsert[2].DateTimeTestValue, LongTestValue = entitiesToUpsert[2].LongTestValue, StringTestValue = existingEntities[1].StringTestValue },
                new TestEntityCompositeKey { IdPartA = "TO BE UPDATED2", IdPartB = "B", IntTestValue = entitiesToUpsert[3].IntTestValue, BoolTestValue = existingEntities[2].BoolTestValue, DateTimeTestValue = entitiesToUpsert[3].DateTimeTestValue, LongTestValue = entitiesToUpsert[3].LongTestValue, StringTestValue = existingEntities[2].StringTestValue },
            };

            // Exclude all fields which are non-nullable
            ExclusionBuilder<TestEntityCompositeKey> insertExclusionBuilder = new ExclusionBuilder<TestEntityCompositeKey>()
                .Exclude(x => x.StringTestValue, x => x.NullableDateTimeTestValue, x => x.NullableGuidValue);

            ExclusionBuilder<TestEntityCompositeKey> updateExclusionBuilder = new ExclusionBuilder<TestEntityCompositeKey>()
                .Exclude(x => x.BoolTestValue, x => x.StringTestValue)
                .Exclude(nameof(TestEntityCompositeKey.GuidValue));

            // Invoke the method and check that the result is the expected entities
            IUpsertResult<TestEntityCompositeKey> result = await context.UpsertAsync(
                entitiesToUpsert,
                insertClusivityBuilder: insertExclusionBuilder,
                updateClusivityBuilder: updateExclusionBuilder);

            result.UpdatedEntities.Should().BeEquivalentTo(new[] { (existingEntities[1], expectedEntitiesInTargetAfterSync[4]), (existingEntities[2], expectedEntitiesInTargetAfterSync[5]) });
            result.InsertedEntities.Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync.Where(entity => entity.IdPartA.StartsWith("TO BE INSERTED")));

            // Then check that the the changes were synced to the db. The resulting table should not touch entries which did not match, nor should it contain non-included fields
            context.TestEntitiesWithCompositeKey.ToList().Should().BeEquivalentTo(expectedEntitiesInTargetAfterSync);
        }
    }
}
