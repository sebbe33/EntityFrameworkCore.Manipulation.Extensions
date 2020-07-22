using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace EntityFrameworkCore.Manipulation.Extensions
{
    public class SyncResult<TEntity> : SyncWithoutUpdateResult<TEntity>
        where TEntity : class
    {
        public SyncResult(
            IReadOnlyCollection<TEntity> deletedEntities,
            IReadOnlyCollection<TEntity> insertedEntities,
            IReadOnlyCollection<(TEntity OldValue, TEntity NewValue)> updatedEntities)
            : base(deletedEntities, insertedEntities)
            => this.UpdatedEntities = updatedEntities;

        public IReadOnlyCollection<(TEntity OldValue, TEntity NewValue)> UpdatedEntities { get; }
    }
}
