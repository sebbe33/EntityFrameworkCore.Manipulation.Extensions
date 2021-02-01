using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
	internal static class ExpressionExtensions
	{
		public static PropertyInfo GetPropertyInfoFromExpression<TEntity>(this Expression<Func<TEntity, object>> propertyExpression)
		{
			MemberExpression memberExpression = null;

			if (propertyExpression.Body is UnaryExpression unaryExpression && unaryExpression.Operand is MemberExpression)
			{
				memberExpression = (MemberExpression)unaryExpression.Operand;
			}
			else if (propertyExpression.Body is MemberExpression)
			{
				memberExpression = (MemberExpression)propertyExpression.Body;
			}

			if (memberExpression == null)
			{
				throw new ArgumentException($"Could not extract property information from expression {propertyExpression}");
			}

			return (PropertyInfo)memberExpression.Member;
		}
	}
}
