using System;
using System.ComponentModel.DataAnnotations;

namespace EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers
{
    public class InlineTestChildEntitiyCompositeKey : ITestEntityCompositeKey
	{
		public string IdPartA { get; set; }

		public string IdPartB { get; set; }

		public int IntTestValue { get; set; }

		public bool BoolTestValue { get; set; }

		public int ExtendedProperty { get; set; }

		public DateTime DateTimeTestValue { get; set; }

		public TestEnum EnumValue { get; set; }

		public Guid GuidValue { get; set; }

		public long LongTestValue { get; set; }

		public DateTime? NullableDateTimeTestValue { get; set; }

		public Guid? NullableGuidValue { get; set; }
	}
}
