namespace EntityFrameworkCore.Manipulation.Extensions.Configuration
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using System.Globalization;

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

        /// <summary>
        /// Gets or sets a value indicating whether to create/use memory-optimized (in-memory OLTP) table types when using
        /// table values parameters. Using memory-optimized table types means that the input data is stored in memory, rather
        /// than in the temp db on disk. This option
        /// </summary>
        public bool UseMemoryOptimizedTableTypes { get; set; } = true;

        /// <summary>
        /// Gets or sets the default Hash Index Bucket Count used when creating a Hash Index for a Memory-Optimized Table Type.
        /// The general guidance is for this to be set to 1.5x - 2x the number of estimated max rows used as input to any of
        /// the extension methods for this lib. You can set the bucket count for individual entity types by utilizing
        /// <see cref="HashIndexBucketCountsByEntityType"/>. For more information, refer to
        /// <see cref="https://docs.microsoft.com/en-us/sql/relational-databases/sql-server-index-design-guide?view=sql-server-ver15#configuring_bucket_count"/>.
        /// </summary>
        public int DefaultHashIndexBucketCount { get; set; } = 1000;

        /// <summary>
        /// Gets or sets a dictionary of Hash Index Bucket Counts to Entitiy Types. This can be used to specifiy the Bucket Count
        /// when creating a Hash Index for a Memory-Optimized Table Type for a specific Entity Type. If a count for a given type
        /// is not set, the <see cref="DefaultHashIndexBucketCount"/> will be used. The general guidance is for this to be set to
        /// 1.5x - 2x the number of estimated max rows used as input to any of the extension methods for this lib. For more information, refer to
        /// <see cref="https://docs.microsoft.com/en-us/sql/relational-databases/sql-server-index-design-guide?view=sql-server-ver15#configuring_bucket_count"/>.
        /// </summary>
        public Dictionary<string, int> HashIndexBucketCountsByEntityType { get; set; } = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

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
