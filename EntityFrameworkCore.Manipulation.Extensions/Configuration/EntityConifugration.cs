namespace EntityFrameworkCore.Manipulation.Extensions.Configuration
{
    using System;
    using System.ComponentModel.DataAnnotations;

    /// <summary>
    /// Entity-specific configuration, controlling indexing, use of memory-optimized types, etc. for a given entity.
    /// </summary>
    public class EntityConifugration
    {
        /// <summary>
        /// Gets or sets the row threhold for when Table Valued Parameters should be used for the referenced entity.
        /// When the number of rows in an input collection to any of the extension methods exceeds this number,
        /// Table Valued Parameters will be used instead of individual parameters.
        ///
        /// If <see cref="UseTableValuedParametersParameterCountTreshold"/> is encountered first
        /// </summary>
        /// <remarks>If not set, the value for <see cref="SqlServerManipulationExtensionsConfiguration.DefaultUseTableValuedParametersRowTreshold"/> will be used.</remarks>
        [Range(0, 2000)]
        public int? UseTableValuedParametersRowTreshold { get; set; }

        /// <summary>
        /// Gets or sets the parameter count threhold for when Table Valued Parameters should be used for the referenced entity.
        /// When the number of parameters, each representing a property of an entity in an input collection to
        /// any of the extension methods exceeds this number, Table Valued Parameters will be used instead of individual parameters.
        /// </summary>
        /// <remarks>If not set, the value for <see cref="SqlServerManipulationExtensionsConfiguration.DetaultUseTableValuedParametersParameterCountTreshold"/> will be used.</remarks>
        [Range(0, 2000)]
        public int? UseTableValuedParametersParameterCountTreshold { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to create/use memory-optimized (in-memory OLTP) table types when using
        /// table values parameters for the referenced entity type. Using memory-optimized table types means that the input data
        /// is stored in memory, rather than in the temp db on disk.
        /// </summary>
        /// <remarks>If not set, the value for <see cref="SqlServerManipulationExtensionsConfiguration.UseMemoryOptimizedTableTypes"/> will be used.</remarks>
        public bool? UseMemoryOptimizedTableTypes { get; set; }

        /// <summary>
        /// Gets or sets the Hash Index Bucket Count used when creating a Hash Index for a Memory-Optimized Table Type for the referenced entity type.
        /// The general guidance is for this to be set to 1.5x - 2x the number of estimated max rows used as input to any of
        /// the extension methods for this lib. You can set the bucket count for individual entity types by utilizing
        /// <see cref="HashIndexBucketCountsByEntityType"/>. For more information, refer to
        /// <see cref="https://docs.microsoft.com/en-us/sql/relational-databases/sql-server-index-design-guide?view=sql-server-ver15#configuring_bucket_count"/>.
        /// </summary>
        /// <remarks>If not set, the value for <see cref="SqlServerManipulationExtensionsConfiguration.DefaultHashIndexBucketCount"/> will be used.</remarks>
        public int? HashBucketSizetHashIndexBucketCount { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the entity type has triggers defined. The library uses OUTPUT statements to return back the
        /// modified data. The OUTPUT statement does not work out-of-box with triggers; the output has to be placed into a temp table and then return.
        /// By registering an entity type here, that's what will happen. Note that if the trigger on an entity modifies the effected entities,
        /// the latest state of the entity will not be returned. Only the output from the actual library operation will be returned.
        /// </summary>
        public bool HasTrigger { get; set; } = false;

        /// <summary>
        /// Gets or sets a value indicating whether to utilize SQL Server's MERGE statement for upserts and syncs, or whether to
        /// used individual INSERT, UPDATE, and DELETE statements for the referenced entity type.. The merge statement may increase performance for upserts and syncs,
        /// especially in regards to IO. It may also decrease performance in certain cases. This setting allows the consumer of the
        /// library to test which use cases is best for their specific scenario.
        /// </summary>
        /// <remarks>If not set, the value for <see cref="SqlServerManipulationExtensionsConfiguration.UseMerge"/> will be used.</remarks>
        public bool? UseMerge { get; set; }

        /// <summary>
        /// Gets or sets a value indicating what type of index to use for the table type uses for table-valued parameters for the
        /// referenced entity type.
        /// </summary>
        /// <remarks>If not set, the value for <see cref="SqlServerManipulationExtensionsConfiguration.DefaultTableTypeIndex"/> will be used.</remarks>
        public SqlServerTableTypeIndex? TableTypeIndex { get; set; }

        /// <summary>
        /// Gets or sets the interceptor to be used when processing the type of entity. The interceptor can be used to re-configure
        /// the table type schema definition before the type is being created.
        /// </summary>
        public ITableValuedParameterInterceptor TableValuedParameterInterceptor { get; set; }
    }
}
