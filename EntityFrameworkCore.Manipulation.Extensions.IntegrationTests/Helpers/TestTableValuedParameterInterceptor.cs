using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers
{
    internal class TestTableValuedParameterInterceptor : ITableValuedParameterInterceptor
    {
        private readonly Dictionary<string, string> propertyTypeOverrides = new Dictionary<string, string>
        {
            { nameof(TestEntity.StringTestValue), "nvarchar(100)" }
        };

        public IEnumerable<IInterceptedProperty> InterceptProperties(IEnumerable<IProperty> properties) =>
            properties.Select(property => new InterceptedProperty
            {
                ColumnName = property.GetColumnName(),
                ColumnType = propertyTypeOverrides.GetValueOrDefault(property.PropertyInfo.Name, property.GetColumnType()),
            });

        private class InterceptedProperty : IInterceptedProperty
        {
            public string ColumnName { get; set; }

            public string ColumnType { get; set; }
        }
    }
}
