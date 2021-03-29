namespace EntityFrameworkCore.Manipulation.Extensions
{
    using System.Collections.Generic;

    public interface ISyncWithoutUpdateResult<TEntity>
        where TEntity : class
    {
        public IReadOnlyCollection<TEntity> DeletedEntities { get; }

        public IReadOnlyCollection<TEntity> InsertedEntities { get; }
    }
}
