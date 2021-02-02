using System;

namespace EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers
{
	public interface ITestEntityCompositeKey
	{
		bool BoolTestValue { get; set; }
		DateTime DateTimeTestValue { get; set; }
		TestEnum EnumValue { get; set; }
		Guid GuidValue { get; set; }
		string IdPartA { get; set; }
		string IdPartB { get; set; }
		int IntTestValue { get; set; }
		long LongTestValue { get; set; }
		DateTime? NullableDateTimeTestValue { get; set; }
		Guid? NullableGuidValue { get; set; }
	}
}