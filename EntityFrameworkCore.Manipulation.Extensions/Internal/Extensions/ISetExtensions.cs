namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
    using System;
    using System.Collections.Generic;
    using System.Text;

    internal static class ISetExtensions
    {
        public static void AddRange<TItem>(this ISet<TItem> source, IEnumerable<TItem> items)
        {
            foreach (TItem item in items)
            {
                source.Add(item);
            }
        }
    }
}
