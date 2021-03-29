namespace EntityFrameworkCore.Manipulation.Extensions
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Configuration for the EntityFrameworkCore.Manipulation.Extensions library.
    /// </summary>
    public static class ManipulationExtensionsConfiguration
    {
        internal static readonly Dictionary<Type, ITableValuedParameterInterceptor> TvpInterceptors = new Dictionary<Type, ITableValuedParameterInterceptor>();

        /// <summary>
        /// Registers the <paramref name="interceptor"/> to be used when processing <typeparamref name="TEntity"/> entities.
        /// </summary>
        /// <typeparam name="TEntity">The type of entity to intercept.</typeparam>
        /// <param name="interceptor">An interceptor instance.</param>
        /// <returns><c>true</c> if there was already an interceptor registered for <typeparamref name="TEntity"/>, <c>false</c> otherwise.</returns>
        public static bool AddTableValuedParameterInterceptor<TEntity>(ITableValuedParameterInterceptor interceptor)
        {
            bool existing = TvpInterceptors.ContainsKey(typeof(TEntity));
            TvpInterceptors[typeof(TEntity)] = interceptor ?? throw new ArgumentNullException(nameof(interceptor));
            return existing;
        }
    }
}
