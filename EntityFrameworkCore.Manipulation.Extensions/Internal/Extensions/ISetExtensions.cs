using System;
using System.Collections.Generic;
using System.Text;

namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
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
