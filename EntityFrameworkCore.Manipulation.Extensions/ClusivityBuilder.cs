namespace EntityFrameworkCore.Manipulation.Extensions
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Linq.Expressions;
    using EntityFrameworkCore.Manipulation.Extensions.Internal;
    using EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions;
    using Microsoft.EntityFrameworkCore.Metadata;

    public interface IClusivityBuilder<TEntity>
    {
        internal IProperty[] Build(IEnumerable<IProperty> allAvailableProperties);
    }

    public class InclusionBuilder<TEntity> : IClusivityBuilder<TEntity>
    {
        private readonly ISet<Expression<Func<TEntity, object>>> propertyExpressions = new HashSet<Expression<Func<TEntity, object>>>();
        private readonly ISet<string> propertyNames = new HashSet<string>();

        /// <summary>
        /// Includes the given set or properties (columns).
        /// </summary>
        /// <param name="propertyExpressions">A set of properties to include.</param>
        /// <returns>Self-reference to the builder.</returns>
        public InclusionBuilder<TEntity> Include(params Expression<Func<TEntity, object>>[] propertyExpressions)
        {
            if (propertyExpressions == null)
            {
                throw new ArgumentNullException(nameof(propertyExpressions));
            }

            this.propertyExpressions.AddRange(propertyExpressions);
            return this;
        }

        /// <summary>
        /// Includes the given set or properties (columns).
        /// </summary>
        /// <param name="propertyNames">A set of properties to include.</param>
        /// <returns>Self-reference to the builder.</returns>
        public InclusionBuilder<TEntity> Include(params string[] propertyNames)
        {
            if (propertyNames == null)
            {
                throw new ArgumentNullException(nameof(propertyNames));
            }

            this.propertyNames.AddRange(propertyNames);
            return this;
        }

        IProperty[] IClusivityBuilder<TEntity>.Build(IEnumerable<IProperty> allAvailableProperties)
        {
            IEnumerable<IProperty> includedProperties = this.propertyExpressions
                .GetPropertiesFromExpressions(allAvailableProperties)
                .Concat(this.propertyNames.GetPropertiesFromPropertyNames(allAvailableProperties));

            IProperty[] finalInclusion = allAvailableProperties
                .Intersect(includedProperties)
                .Distinct()
                .ToArray();

            return finalInclusion.Length == 0 ?
                throw new InvalidOperationException("There needs to be at least one non primary-key property included that's mapped to the table - found 0")
                :
                finalInclusion;
        }
    }

    public class ExclusionBuilder<TEntity> : IClusivityBuilder<TEntity>
    {
        private readonly ISet<Expression<Func<TEntity, object>>> propertyExpressions = new HashSet<Expression<Func<TEntity, object>>>();
        private readonly ISet<string> propertyNames = new HashSet<string>();

        /// <summary>
        /// Excludes the given set or properties (columns).
        /// </summary>
        /// <param name="propertyExpressions">A set of properties to include.</param>
        /// <returns>Self-reference to the builder.</returns>
        public ExclusionBuilder<TEntity> Exclude(params Expression<Func<TEntity, object>>[] propertyExpressions)
        {
            if (propertyExpressions == null)
            {
                throw new ArgumentNullException(nameof(propertyExpressions));
            }

            this.propertyExpressions.AddRange(propertyExpressions);
            return this;
        }

        /// <summary>
        /// Excludes the given set or properties (columns).
        /// </summary>
        /// <param name="propertyNames">A set of properties to include.</param>
        /// <returns>Self-reference to the builder.</returns>
        public ExclusionBuilder<TEntity> Exclude(params string[] propertyNames)
        {
            if (propertyNames == null)
            {
                throw new ArgumentNullException(nameof(propertyNames));
            }

            this.propertyNames.AddRange(propertyNames);
            return this;
        }

        IProperty[] IClusivityBuilder<TEntity>.Build(IEnumerable<IProperty> allAvailableProperties)
        {
            IEnumerable<IProperty> excludedProperties = this.propertyExpressions
                .GetPropertiesFromExpressions(allAvailableProperties)
                .Concat(this.propertyNames.GetPropertiesFromPropertyNames(allAvailableProperties));

            IProperty[] finalInclusion = allAvailableProperties
                .Except(excludedProperties)
                .Distinct()
                .ToArray();

            return finalInclusion.Length == 0 ?
                throw new InvalidOperationException("There needs to be at least one non primary-key property included that's mapped to the table - found 0")
                :
                finalInclusion;
        }
    }
}
