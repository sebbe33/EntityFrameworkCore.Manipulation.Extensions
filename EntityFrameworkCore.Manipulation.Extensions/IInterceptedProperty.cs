namespace EntityFrameworkCore.Manipulation.Extensions
{
    /// <summary>
    /// An intercepted property definition.
    /// </summary>
    public interface IInterceptedProperty
    {
        /// <summary>
        /// Name of the column in the table.
        /// </summary>
        string ColumnName { get; }

        /// <summary>
        /// Type of the column in the table.
        /// </summary>
        string ColumnType { get; }
    }
}
