using System.Collections.Generic;

namespace EntityFrameworkCore.Manipulation.Extensions
{
    public interface ISyncWithoutUpdateResult<TEntity>
        where TEntity : class
    {
        public IReadOnlyCollection<TEntity> DeletedEntities { get; }

        public IReadOnlyCollection<TEntity> InsertedEntities { get; }
    }
}
