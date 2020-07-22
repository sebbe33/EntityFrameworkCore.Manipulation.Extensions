using System.Collections.Generic;

namespace EntityFrameworkCore.Manipulation.Extensions
{
    public interface ISyncResult<TEntity> 
        where TEntity : class
    {
        IReadOnlyCollection<TEntity> DeletedEntities { get; }

        IReadOnlyCollection<TEntity> InsertedEntities { get; }

        IReadOnlyCollection<(TEntity OldValue, TEntity NewValue)> UpdatedEntities { get; }
    }
}