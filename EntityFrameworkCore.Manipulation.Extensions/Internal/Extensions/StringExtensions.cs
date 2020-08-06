using System.Security.Cryptography;
using System.Text;

namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
    public static class StringExtensions
    {
        public static string GetDeterministicStringHash(this string subject)
        {
            using var hashingScheme = SHA1.Create();
            byte[] bytes = hashingScheme.ComputeHash(Encoding.UTF8.GetBytes(subject));

            StringBuilder builder = new StringBuilder(40);
            foreach (var b in bytes)
            {
                builder.Append(b.ToString("x2"));
            }
            return builder.ToString();
        }
    }
}
