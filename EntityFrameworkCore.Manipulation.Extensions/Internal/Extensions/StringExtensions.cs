namespace EntityFrameworkCore.Manipulation.Extensions.Internal.Extensions
{
    using System.Security.Cryptography;
    using System.Text;

    public static class StringExtensions
    {
        public static string GetDeterministicStringHash(this string subject)
        {
            using var hashingScheme = SHA1.Create();
            byte[] bytes = hashingScheme.ComputeHash(Encoding.UTF8.GetBytes(subject));

            var builder = new StringBuilder(40);
            foreach (byte b in bytes)
            {
                builder.Append(b.ToString("x2"));
            }
            return builder.ToString();
        }
    }
}
