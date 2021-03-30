namespace EntityFrameworkCore.Manipulation.Extensions.UnitTests
{
    using EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers;
    using FluentAssertions;
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.Linq;
    using System.Threading.Tasks;

    [TestClass]
    public class UpdateTests
    {
        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        public async Task UpdateAsync_ShouldReturnEmptyCollection_WhenThereAreNoEntitiesInDbNorInput(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, testConfiguration: testConfiguration); // Note: no seed data => no entities exist

            // Invoke the method and check that the result is empty
            IReadOnlyCollection<TestEntityCompositeKey> result = await context.UpdateAsync(Array.Empty<TestEntityCompositeKey>());
            result.Should().BeEmpty();

            // Validate in the DB
            context.TestEntitiesWithCompositeKey.Should().BeEmpty();
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        public async Task UpdateAsync_ShouldReturnEmptyCollection_WhenThereAreNoEntitiesInInput(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntityCompositeKey[] existingEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Should not be touched 1", IdPartB = "B", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                new TestEntityCompositeKey { IdPartA = "Should not be touched 2", IdPartB ="B", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
            };

            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, seedData: existingEntities, testConfiguration: testConfiguration);

            // Invoke the method and check that the result is empty
            IReadOnlyCollection<TestEntityCompositeKey> result = await context.UpdateAsync(Array.Empty<TestEntityCompositeKey>());
            result.Should().BeEmpty();

            // Validate that the DB is untouched
            context.TestEntitiesWithCompositeKey.Should().BeEquivalentTo(existingEntities);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        public async Task UpdateAsync_ShouldReturnEmptyCollection_WhenThereAreNoMatchingEntitiesBasedOnKey(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntityCompositeKey[] existingEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Should not be touched 1", IdPartB = "B", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                new TestEntityCompositeKey { IdPartA = "Should not be touched 2", IdPartB ="B", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
            };

            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, seedData: existingEntities, testConfiguration: testConfiguration);

            // Invoke the method and check that the result is empty
            IReadOnlyCollection<TestEntityCompositeKey> result = await context.UpdateAsync(
                new[]
                {
                    new TestEntityCompositeKey { IdPartA = "Non-matching key 1", IdPartB = "B", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                    new TestEntityCompositeKey { IdPartA = "Non-matching key 2", IdPartB ="B", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                });
            result.Should().BeEmpty();

            // Validate that the DB is untouched
            context.TestEntitiesWithCompositeKey.Should().BeEquivalentTo(existingEntities);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        public async Task UpdateAsync_ShouldReturnEmptyCollection_WhenThereAreNoMatchingEntitiesBasedOnCondition(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntityCompositeKey[] existingEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Should not be touched 1", IdPartB = "B", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                new TestEntityCompositeKey { IdPartA = "Should not be touched 2", IdPartB = "B", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
            };

            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, seedData: existingEntities, testConfiguration: testConfiguration);

            // Invoke the method and check that the result is empty
            IReadOnlyCollection<TestEntityCompositeKey> result = await context.UpdateAsync(
                new[]
                {
                    new TestEntityCompositeKey { IdPartA = "Should not be touched 1", IdPartB = "B", IntTestValue = 0, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                    new TestEntityCompositeKey { IdPartA = "Should not be touched 2", IdPartB = "B", IntTestValue = 0, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                },
                condition: updateEntry => updateEntry.Incoming.IntTestValue > updateEntry.Current.IntTestValue);
            result.Should().BeEmpty();

            // Validate that the DB is untouched
            context.TestEntitiesWithCompositeKey.Should().BeEquivalentTo(existingEntities);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        public async Task UpdateAsync_ShouldReturnUpdatedCollection_WhenAllEntitiesAreMatchingWithoutCondition(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntityCompositeKey[] existingEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Should be updated 1", IdPartB = "B", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                new TestEntityCompositeKey { IdPartA = "Should be updated 2", IdPartB = "B", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
            };
            TestEntityCompositeKey[] expectedEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Should be updated 1", IdPartB = "B", IntTestValue = 561645, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow.AddDays(1), LongTestValue = 781 },
                new TestEntityCompositeKey { IdPartA = "Should be updated 2", IdPartB = "B", IntTestValue = 111, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow.AddDays(1), LongTestValue = 6613 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, seedData: existingEntities, testConfiguration: testConfiguration);

            // Invoke the method and check that the result the updated expected entities
            IReadOnlyCollection<TestEntityCompositeKey> result = await context.UpdateAsync(expectedEntities);
            result.Should().BeEquivalentTo(expectedEntities);

            // Validate that the DB is updated
            context.TestEntitiesWithCompositeKey.Should().BeEquivalentTo(expectedEntities);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        public async Task UpdateAsync_ShouldReturnAffectedUpdatedCollection_WhenASubsetOfEntitiesAreMatchingWithoutCondition(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntityCompositeKey[] existingEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Should be updated 1", IdPartB = "B", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                new TestEntityCompositeKey { IdPartA = "Should not be updated 2", IdPartB = "B", IntTestValue = 56123, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 1231 },
                new TestEntityCompositeKey { IdPartA = "Should be updated 3", IdPartB = "B", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntityCompositeKey { IdPartA = "Should not be updated 4", IdPartB = "B", IntTestValue = 12, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 897546 },
            };
            TestEntityCompositeKey[] expectedEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Should be updated 1", IdPartB = "B", IntTestValue = 561645, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow.AddDays(1), LongTestValue = 781 },
                new TestEntityCompositeKey { IdPartA = "Should be updated 3", IdPartB = "B", IntTestValue = 111, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow.AddDays(1), LongTestValue = 6613 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, seedData: existingEntities, testConfiguration: testConfiguration);

            // Invoke the method and check that the result the updated expected entities
            IReadOnlyCollection<TestEntityCompositeKey> result = await context.UpdateAsync(expectedEntities);
            result.Should().BeEquivalentTo(expectedEntities);

            // Validate that the DB is updated
            context.TestEntitiesWithCompositeKey.Should().BeEquivalentTo(new[] { expectedEntities[0], existingEntities[1], expectedEntities[1], existingEntities[3] });
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        public async Task UpdateAsync_ShouldReturnAffectedUpdatedCollection_WhenASubsetOfEntitiesAreMatchingWithCondition(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntityCompositeKey[] existingEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Should be updated 1", IdPartB = "B", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                new TestEntityCompositeKey { IdPartA = "Should not be updated 2", IdPartB = "B", IntTestValue = 56123, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 1231 },
                new TestEntityCompositeKey { IdPartA = "Should be updated 3", IdPartB = "B", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
                new TestEntityCompositeKey { IdPartA = "Should not be updated 4", IdPartB = "B", IntTestValue = 12, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 897546 },
            };
            TestEntityCompositeKey[] expectedEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Should be updated 1", IdPartB = "B", IntTestValue = 561645231, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow.AddDays(1), LongTestValue = 781 },
                new TestEntityCompositeKey { IdPartA = "Should be updated 3", IdPartB = "B", IntTestValue = 561235164, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow.AddDays(1), LongTestValue = 6613 },
                new TestEntityCompositeKey { IdPartA = "Should not be updated 4", IdPartB = "B", IntTestValue = -1, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 897546 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, seedData: existingEntities, testConfiguration: testConfiguration);

            // Invoke the method and check that the result the updated expected entities
            IReadOnlyCollection<TestEntityCompositeKey> result = await context.UpdateAsync(
                expectedEntities,
                condition: x => x.Incoming.IntTestValue > x.Current.IntTestValue); // Only update if IntTestValue is greater than the incoming value, which rules out "Should not be updated 4"
            result.Should().BeEquivalentTo(expectedEntities.Where(x => x.IdPartA != "Should not be updated 4"));

            // Validate that the DB is updated
            context.TestEntitiesWithCompositeKey.Should().BeEquivalentTo(new[] { expectedEntities[0], existingEntities[1], expectedEntities[1], existingEntities[3] });
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerMemoryOptimizedTableTypes)]
        [DataRow(DbProvider.SqlServer, TestConfiguration.SqlServerRegularTableTypes)]
        public async Task UpdateAsync_ShouldReturnCollectionWithOnlyIncludedPropertiesUpdated_WhenEntitiesMatchAndIncludedPropertyExpresionsArePassed(DbProvider provider, TestConfiguration testConfiguration = TestConfiguration.Default)
        {
            TestEntityCompositeKey[] existingEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Should not be updated 1", IdPartB = "B", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                new TestEntityCompositeKey { IdPartA = "Should not be updated 2", IdPartB = "B", IntTestValue = 56123, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 1231 },
                new TestEntityCompositeKey { IdPartA = "Should be updated 3", IdPartB = "B", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
            };
            TestEntityCompositeKey[] expectedEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Should not be updated 1", IdPartB = "B", IntTestValue = -1, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow.AddDays(1), LongTestValue = 781 },
                new TestEntityCompositeKey { IdPartA = "Should be updated 3", IdPartB = "B", IntTestValue = 561235164, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow.AddDays(1), LongTestValue = 165465132165 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, seedData: existingEntities, testConfiguration: testConfiguration);

            // Include long and datetime values - they are the only items expected to be updated based on the mocked data.
            InclusionBuilder<TestEntityCompositeKey> inclusionBuilder = new InclusionBuilder<TestEntityCompositeKey>()
                .Include(x => x.LongTestValue)
                .Include(nameof(TestEntityCompositeKey.DateTimeTestValue));

            // Invoke the method and check that the result the updated expected entities
            IReadOnlyCollection<TestEntityCompositeKey> result = await context.UpdateAsync(
                expectedEntities,
                condition: x => x.Incoming.IntTestValue > x.Current.IntTestValue, // Only update if IntTestValue is greater than the incoming value, which rules out "Should not be updated 1"
                clusivityBuilder: inclusionBuilder);

            var expectedUpdatedEntity = new TestEntityCompositeKey
            {
                IdPartA = expectedEntities[1].IdPartA,
                IdPartB = expectedEntities[1].IdPartB,
                IntTestValue = existingEntities[2].IntTestValue, // We did not include this field in the update => it should have its original value
                BoolTestValue = existingEntities[2].BoolTestValue, // We did not include this field in the update => it should have its original value
                DateTimeTestValue = expectedEntities[1].DateTimeTestValue,
                LongTestValue = expectedEntities[1].LongTestValue,
            };

            result.Should().BeEquivalentTo(new[] { expectedUpdatedEntity });

            // Validate that the DB is updated
            context.TestEntitiesWithCompositeKey.Should().BeEquivalentTo(new[] { existingEntities[0], existingEntities[1], expectedUpdatedEntity });
        }

        [TestMethod]
        public async Task UpdateAsync_ShouldReturnAffectedUpdatedCollection_WhenASubsetOfEntitiesAreMatchingWithConditionUsingTvpInterceptor()
        {
            TestInterceptorEntity[] existingEntities = Enumerable.Range(0, 52).Select(id => new TestInterceptorEntity
            {
                Id = id.ToString(CultureInfo.InvariantCulture),
                IntTestValue = id % 2,
                BoolTestValue = false,
                StringTestValue = "short string",
            }).ToArray();
            TestInterceptorEntity[] expectedEntities = Enumerable.Range(0, 52).Select(id => new TestInterceptorEntity
            {
                Id = id.ToString(CultureInfo.InvariantCulture),
                IntTestValue = 1,
                BoolTestValue = true,
                // The string field has a max length of 25 chars set with an attribute.
                // We're extending the max length with the TvpInterceptor by changing the type it will have in the temporary table.
                StringTestValue = "a really long string which is longer than the limit we have on the property",
            }).ToArray();

            var interceptedProperties = new List<IInterceptedProperty>();
            TestTableValuedParameterInterceptor.TestCallback = (properties) => interceptedProperties.AddRange(properties);

            // We're only using Table Valued Parameters in SqlServer
            using TestDbContext context = await ContextFactory.GetDbContextAsync(DbProvider.SqlServer, seedData: existingEntities);

            // Make sure we're using TVP
            context.ManipulationExtensionsConfiguration.SqlServerConfiguration.UseTableValuedParametersParameterCountTreshold = 0;

            // Add the test interceptor
            context.ManipulationExtensionsConfiguration.SqlServerConfiguration.AddTableValuedParameterInterceptor<TestInterceptorEntity>(new TestTableValuedParameterInterceptor());

            // Include bool values - they are the only items expected to be updated based on the mocked data.
            InclusionBuilder<TestInterceptorEntity> inclusionBuilder = new InclusionBuilder<TestInterceptorEntity>().Include(x => x.BoolTestValue);

            // Invoke the method and check that the result the updated expected entities
            IReadOnlyCollection<TestInterceptorEntity> result = await context.UpdateAsync(
                expectedEntities,
                condition: x => x.Incoming.IntTestValue == x.Current.IntTestValue, // Only update if IntTestValue is equal to the incoming value
                clusivityBuilder: inclusionBuilder);
            Assert.AreEqual(expectedEntities.Length / 2, result.Count);
            Assert.IsTrue(result.All(r => r.BoolTestValue));

            // Validate that the DB is updated
            context.TestInterceptorEntities.Should().BeEquivalentTo(existingEntities.Select(e => new TestInterceptorEntity
            {
                Id = e.Id,
                IntTestValue = e.IntTestValue,
                BoolTestValue = e.IntTestValue == 1,
                StringTestValue = e.StringTestValue,
            }));

            // Validate the TvpInterceptor has been called and what it returned
            foreach (KeyValuePair<string, string> propertyKvp in TestTableValuedParameterInterceptor.PropertyTypeOverrides)
            {
                Assert.AreEqual(1, interceptedProperties.Count(p => p.ColumnName == propertyKvp.Key && p.ColumnType == propertyKvp.Value));
            }
            Assert.AreEqual(typeof(TestInterceptorEntity).GetProperties().Length, interceptedProperties.Count);
        }

        [DataTestMethod]
        [DataRow(DbProvider.Sqlite)]
        [DataRow(DbProvider.SqlServer)]
        public async Task UpdateAsync_ShouldReturnCollectionWithOnlyNonExcludedPropertiesUpdated_WhenEntitiesMatchAndIncludedPropertyNamesArePassed(DbProvider provider)
        {
            TestEntityCompositeKey[] existingEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Should not be updated 1", IdPartB = "B", IntTestValue = 561645, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 54123 },
                new TestEntityCompositeKey { IdPartA = "Should not be updated 2", IdPartB = "B", IntTestValue = 56123, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 1231 },
                new TestEntityCompositeKey { IdPartA = "Should be updated 3", IdPartB = "B", IntTestValue = 111, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow, LongTestValue = 65465132165 },
            };
            TestEntityCompositeKey[] expectedEntities = new[]
            {
                new TestEntityCompositeKey { IdPartA = "Should not be updated 1", IdPartB = "B", IntTestValue = -1, BoolTestValue = true, DateTimeTestValue = DateTime.UtcNow.AddDays(1), LongTestValue = 781 },
                new TestEntityCompositeKey { IdPartA = "Should be updated 3", IdPartB = "B", IntTestValue = 561235164, BoolTestValue = false, DateTimeTestValue = DateTime.UtcNow.AddDays(1), LongTestValue = 165465132165 },
            };
            using TestDbContext context = await ContextFactory.GetDbContextAsync(provider, seedData: existingEntities);

            // Exclude int and bool values - they are expected to not be updated based on the mocked data.
            ExclusionBuilder<TestEntityCompositeKey> exclusionBuilder = new ExclusionBuilder<TestEntityCompositeKey>()
                .Exclude(x => x.IntTestValue, x => x.BoolTestValue);

            // Invoke the method and check that the result the updated expected entities
            IReadOnlyCollection<TestEntityCompositeKey> result = await context.UpdateAsync(
                expectedEntities,
                condition: x => x.Incoming.IntTestValue > x.Current.IntTestValue, // Only update if IntTestValue is greater than the incoming value, which rules out "Should not be updated 1"
                clusivityBuilder: exclusionBuilder);

            var expectedUpdatedEntity = new TestEntityCompositeKey
            {
                IdPartA = expectedEntities[1].IdPartA,
                IdPartB = expectedEntities[1].IdPartB,
                IntTestValue = existingEntities[2].IntTestValue, // We did not include this field in the update => it should have its original value
                BoolTestValue = existingEntities[2].BoolTestValue, // We did not include this field in the update => it should have its original value
                DateTimeTestValue = expectedEntities[1].DateTimeTestValue,
                LongTestValue = expectedEntities[1].LongTestValue,
            };

            result.Should().BeEquivalentTo(new[] { expectedUpdatedEntity });

            // Validate that the DB is updated
            context.TestEntitiesWithCompositeKey.Should().BeEquivalentTo(new[] { existingEntities[0], existingEntities[1], expectedUpdatedEntity });
        }
    }
}
