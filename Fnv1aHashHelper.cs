using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace AzureStorageRestClient
{
    /// <summary>
    /// Fast, non-cryptographic hash function helper.
    /// </summary>
    /// <remarks>
    /// See https://en.wikipedia.org/wiki/Fowler%E2%80%93Noll%E2%80%93Vo_hash_function.
    /// Tested with all sites from production GeoMaster and random guids. The result was good distribution.
    /// </remarks>
#if !NET5_0_OR_GREATER
    [CLSCompliant(false)]
#endif
    public static class Fnv1aHashHelper
    {
        private const uint FnvPrime = unchecked(16777619);
        public const uint FnvOffsetBasis = unchecked(2166136261);

        public static uint ComputeHash(string value, Encoding encoding = null, uint hash = FnvOffsetBasis)
        {
            byte[] bytes = (encoding ?? Encoding.UTF8).GetBytes(value);
            return ComputeHash(bytes, hash);
        }

        public static uint ComputeHash(byte[] array, uint hash = FnvOffsetBasis)
        {
            for (var i = 0; i < array.Length; i++)
            {
                unchecked
                {
                    hash ^= array[i];
                    hash *= FnvPrime;
                }
            }

            return hash;
        }

        public static uint ComputeHash(Stream stream, int length, uint hash = FnvOffsetBasis)
        {
            var total = 0;
            var array = new byte[8 * 1_024];
            while (total < length)
            {
                var read = stream.Read(array, 0, Math.Min(array.Length, length - total));
                for (var i = 0; i < read; i++)
                {
                    unchecked
                    {
                        hash ^= array[i];
                        hash *= FnvPrime;
                    }
                }

                total += read;
            }

            return hash;
        }

        public static int NormalizeAndComputeHash(IEnumerable<string> values)
        {
            var value = String.Join(";", values.Select(v => v.ToUpperInvariant()).OrderBy(v => v));
            return unchecked((int)ComputeHash(value));
        }
    }
}
