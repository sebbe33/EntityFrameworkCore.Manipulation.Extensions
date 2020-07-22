using System.Collections.Generic;

namespace EntityFrameworkCore.Manipulation.Extensions.Internal
{
    internal class SyncResult<TEntity> : ISyncResult<TEntity>, ISyncWithoutUpdateResult<TEntity>, IUpsertResult<TEntity>
        where TEntity : class
    {
        public SyncResult(
            IReadOnlyCollection<TEntity> deletedEntities,
            IReadOnlyCollection<TEntity> insertedEntities,
            IReadOnlyCollection<(TEntity OldValue, TEntity NewValue)> updatedEntities)
        {
            this.DeletedEntities = deletedEntities;
            this.InsertedEntities = insertedEntities;
            this.UpdatedEntities = updatedEntities;
        }

        public IReadOnlyCollection<TEntity> DeletedEntities { get; }

        public IReadOnlyCollection<TEntity> InsertedEntities { get; }

        public IReadOnlyCollection<(TEntity OldValue, TEntity NewValue)> UpdatedEntities { get; }
    }
}
