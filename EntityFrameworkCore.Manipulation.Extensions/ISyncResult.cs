namespace EntityFrameworkCore.Manipulation.Extensions
{
    using System.Collections.Generic;

    public interface ISyncResult<TEntity>
        where TEntity : class
    {
        IReadOnlyCollection<TEntity> DeletedEntities { get; }

        IReadOnlyCollection<TEntity> InsertedEntities { get; }

        IReadOnlyCollection<(TEntity OldValue, TEntity NewValue)> UpdatedEntities { get; }
    }
}