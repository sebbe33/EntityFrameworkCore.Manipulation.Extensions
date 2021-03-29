namespace EntityFrameworkCore.Manipulation.Extensions
{
    using System.Collections.Generic;

    public interface IUpsertResult<TEntity>
        where TEntity : class
    {
        IReadOnlyCollection<TEntity> InsertedEntities { get; }

        IReadOnlyCollection<(TEntity OldValue, TEntity NewValue)> UpdatedEntities { get; }
    }
}