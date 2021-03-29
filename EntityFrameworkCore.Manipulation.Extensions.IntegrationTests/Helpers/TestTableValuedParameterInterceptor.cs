namespace EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Microsoft.EntityFrameworkCore;
    using Microsoft.EntityFrameworkCore.Metadata;

    internal class TestTableValuedParameterInterceptor : ITableValuedParameterInterceptor
    {
        public static Action<IEnumerable<IInterceptedProperty>> TestCallback;

        public static readonly Dictionary<string, string> PropertyTypeOverrides = new Dictionary<string, string>
        {
            { nameof(TestEntity.StringTestValue), "nvarchar(100)" }
        };

        public IEnumerable<IInterceptedProperty> OnCreatingProperties(IEnumerable<IProperty> properties)
        {
            var interceptedProperties = properties.Select(property => new InterceptedProperty
            {
                ColumnName = property.GetColumnName(),
                ColumnType = PropertyTypeOverrides.GetValueOrDefault(property.PropertyInfo.Name, property.GetColumnType()),
            }).ToList();
            TestCallback?.Invoke(interceptedProperties);
            return interceptedProperties;
        }

        private class InterceptedProperty : IInterceptedProperty
        {
            public string ColumnName { get; set; }

            public string ColumnType { get; set; }
        }
    }
}
