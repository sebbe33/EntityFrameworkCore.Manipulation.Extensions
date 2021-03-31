namespace EntityFrameworkCore.Manipulation.Extensions.IntegrationTests.Helpers
{
    using System.ComponentModel.DataAnnotations;
    using System.ComponentModel.DataAnnotations.Schema;

    public class TestInterceptorEntity
    {
        [Key]
        public string Id { get; set; }

        public int IntTestValue { get; set; }

        public bool BoolTestValue { get; set; }

        public long LongTestValue { get; set; }

        [Column(TypeName = "nvarchar(25)")]
        public string StringTestValue { get; set; }
    }
}
