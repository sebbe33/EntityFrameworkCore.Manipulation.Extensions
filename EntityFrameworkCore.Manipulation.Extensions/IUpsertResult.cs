using System.Collections.Generic;

namespace EntityFrameworkCore.Manipulation.Extensions
{
    public interface IUpsertResult<TEntity> 
        where TEntity : class
    {
        IReadOnlyCollection<TEntity> InsertedEntities { get; }

        IReadOnlyCollection<(TEntity OldValue, TEntity NewValue)> UpdatedEntities { get; }
    }
}