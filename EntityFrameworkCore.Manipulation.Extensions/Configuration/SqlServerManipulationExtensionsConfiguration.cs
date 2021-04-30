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
        /// If <see cref="DetaultUseTableValuedParametersParameterCountTreshold"/> is encountered first
        /// </summary>
        [Range(0, 2000)]
        public int DefaultUseTableValuedParametersRowTreshold { get; set; } = 50;

        /// <summary>
        /// Gets or sets the parameter count threhold for when Table Valued Parameters should be used.
        /// When the number of parameters, each representing a property of an entity in an input collection to
        /// any of the extension methods exceeds this number, Table Valued Parameters will be used instead of individual parameters.
        /// </summary>
        [Range(0, 2000)]
        public int DetaultUseTableValuedParametersParameterCountTreshold { get; set; } = 500;

        /// <summary>
        /// Gets or sets a value indicating whether to create/use memory-optimized (in-memory OLTP) table types when using
        /// table values parameters. Using memory-optimized table types means that the input data is stored in memory, rather
        /// than in the temp db on disk.
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
        /// Gets or sets a value indicating whether to utilize SQL Server's MERGE statement for upserts and syncs, or whether to
        /// used individual INSERT, UPDATE, and DELETE statements. The merge statement may increase performance for upserts and syncs,
        /// especially in regards to IO. It may also decrease performance in certain cases. This setting allows the consumer of the
        /// library to test which use cases is best for their specific scenario.
        /// </summary>
        public bool UseMerge { get; set; } = true;

        /// <summary>
        /// Gets or sets the default for what type of index to use for the table type uses for table-valued parameters.
        /// </summary>
        public SqlServerTableTypeIndex DefaultTableTypeIndex { get; set; } = SqlServerTableTypeIndex.Default;

        internal IDictionary<Type, EntityConifugration> EntityConfigurations { get; } = new Dictionary<Type, EntityConifugration>();

        /// <summary>
        /// Adds configuration specific for a given type of <typeparamref name="TEntity"/>. This can be used to control
        /// settings on an entity-level.
        /// </summary>
        /// <typeparam name="TEntity">The type of entity.</typeparam>
        /// <param name="entityConifugration">The entity-specific configuration.</param>
        /// <returns>True if the configuration was added, false if it was already present.</returns>
        public bool AddEntityConifugration<TEntity>(EntityConifugration entityConifugration) =>
            this.EntityConfigurations.TryAdd(typeof(TEntity), entityConifugration ?? throw new ArgumentNullException(nameof(entityConifugration)));
    }
}
