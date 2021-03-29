namespace EntityFrameworkCore.Manipulation.Extensions.Configuration
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;

    /// <summary>
    /// Configuration for the EntityFrameworkCore.Manipulation.Extensions library.
    /// </summary>
    public class SqlServerManipulationExtensionsConfiguration
    {
        /// <summary>
        /// Gets or sets the row threhold for when Table Valued Parameters should be used.
        /// When the number of rows in an input collection to any of the extension methods exceeds this number,
        /// Table Valued Parameters will be used instead of individual parameters.
        ///
        /// If <see cref="UseTableValuedParametersParameterCountTreshold"/> is encountered first
        /// </summary>
        [Range(0, 2000)]
        public int UseTableValuedParametersRowTreshold { get; set; } = 50;

        /// <summary>
        /// Gets or sets the parameter count threhold for when Table Valued Parameters should be used.
        /// When the number of parameters, each representing a property of an entity in an input collection to
        /// any of the extension methods exceeds this number, Table Valued Parameters will be used instead of individual parameters.
        /// </summary>
        [Range(0, 2000)]
        public int UseTableValuedParametersParameterCountTreshold { get; set; } = 500;

        internal Dictionary<Type, ITableValuedParameterInterceptor> TvpInterceptors { get; } = new Dictionary<Type, ITableValuedParameterInterceptor>();

        /// <summary>
        /// Registers the <paramref name="interceptor"/> to be used when processing <typeparamref name="TEntity"/> entities.
        /// </summary>
        /// <typeparam name="TEntity">The type of entity to intercept.</typeparam>
        /// <param name="interceptor">An interceptor instance.</param>
        /// <returns><c>true</c> if there was already an interceptor registered for <typeparamref name="TEntity"/>, <c>false</c> otherwise.</returns>
        public bool AddTableValuedParameterInterceptor<TEntity>(ITableValuedParameterInterceptor interceptor) =>
            this.TvpInterceptors.TryAdd(typeof(TEntity), interceptor ?? throw new ArgumentNullException(nameof(interceptor)));
    }
}
