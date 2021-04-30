namespace EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers
{

    public enum TestConfiguration
    {
        Default,
        SqlServerRegularTableTypes,
        SqlServerRegularTableTypesWithClusteredIndex,
        SqlServerRegularTableTypesWithNonclusteredIndex,
        SqlServerMemoryOptimizedTableTypes,
        SqlServerMemoryOptimizedTableTypesWithNonclusteredIndex,
        SqlServerOutputInto,
        SqlServerMergeSync,
        SqlServerSimpleStatementsSync
    }
}
