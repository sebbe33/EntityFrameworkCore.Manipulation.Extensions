using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EntityFrameworkCore.Manipulation.Extensions.Internal
{
    internal class DefaultTableValuedParameterInterceptor : ITableValuedParameterInterceptor
    {
        public static ITableValuedParameterInterceptor Instance = new DefaultTableValuedParameterInterceptor();

        public IEnumerable<IInterceptedProperty> OnCreatingProperties(IEnumerable<IProperty> properties) =>
            properties.Select(property => new DefaultInterceptedProperty
            {
                ColumnName = property.GetColumnName(),
                ColumnType = property.GetColumnType(),
            });

        private class DefaultInterceptedProperty : IInterceptedProperty
        {
            public string ColumnName { get; set; }

            public string ColumnType { get; set; }
        }
    }
}
