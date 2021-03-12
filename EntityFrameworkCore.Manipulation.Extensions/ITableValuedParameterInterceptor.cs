using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Metadata;

namespace EntityFrameworkCore.Manipulation.Extensions
{
    /// <summary>
    /// An interceptor which allows customizing the configuration of a table valued paramter table.
    /// </summary>
    public interface ITableValuedParameterInterceptor
    {
        /// <summary>
        /// Processes EF <see cref="IProperty"/> into a <see cref="IInterceptedProperty"/> which allows modify it
        /// e.g. the column type on the temporary table.
        /// </summary>
        /// <param name="properties">Entity properties to process.</param>
        /// <returns>Enumeration of <see cref="IInterceptedProperty"/> to be used for temporary table construction.</returns>
        IEnumerable<IInterceptedProperty> OnCreatingProperties(IEnumerable<IProperty> properties);
    }
}
