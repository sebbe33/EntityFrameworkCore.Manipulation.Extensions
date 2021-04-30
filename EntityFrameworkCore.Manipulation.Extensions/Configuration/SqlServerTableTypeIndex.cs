namespace EntityFrameworkCore.Manipulation.Extensions.Configuration
{
    public enum SqlServerTableTypeIndex
    {
        /// <summary>
        /// Default. For a non-memory optimized table type, this defaults to <see cref="NoIndex"/>.
        /// For a memory-optimized table type, this defaults to <see cref="HashIndex"/>
        /// </summary>
        Default = 0,

        /// <summary>
        /// No index. Only available on non-memory optimized table types.
        /// </summary>
        NoIndex = 1,

        /// <summary>
        /// Clustered index.
        /// </summary>
        ClusteredIndex = 2,

        /// <summary>
        /// Non-clustered index.
        /// </summary>
        NonClusteredIndex = 3,

        /// <summary>
        /// Hash index.
        /// </summary>
        HashIndex = 4
    }
}
