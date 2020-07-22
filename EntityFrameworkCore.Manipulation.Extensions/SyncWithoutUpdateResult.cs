using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace EntityFrameworkCore.Manipulation.Extensions
{
    public class SyncWithoutUpdateResult<TEntity>
        where TEntity : class
    {
        public SyncWithoutUpdateResult(
            IReadOnlyCollection<TEntity> deletedEntities, 
            IReadOnlyCollection<TEntity> insertedEntities)
        {
            this.DeletedEntities = deletedEntities;
            this.InsertedEntities = insertedEntities;
        }

        public IReadOnlyCollection<TEntity> DeletedEntities { get; }

        public IReadOnlyCollection<TEntity> InsertedEntities { get; }
    }
}
