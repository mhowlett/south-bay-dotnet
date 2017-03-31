using System;
using System.Collections;

// Derived from:
// https://bloomfilter.codeplex.com/SourceControl/latest#BloomFilter/Filter.cs
// (c) Microsoft
// Microsoft permissive license.

namespace BloomFilter
{
    /// <summary>
    ///     Bloom filter.
    /// </summary>
    public class Filter
    {
        private readonly int _hashFunctionCount;
        private readonly BitArray _hashBits;

        private readonly HashFunction _getHashSecondary = HashString;

        /// <summary>
        ///     Reconstructs a Bloom filter from the provided serialized byte array.
        /// </summary>
        /// <param name="serialized">
        ///     The byte array to reconstruct the bloom filter from.
        /// </param>
        public Filter(byte[] serialized)
        {
            _hashFunctionCount = BitConverter.ToInt32(serialized, 0);
            byte[] bitArrayBytes = new byte[serialized.Length - sizeof(int)];
            Array.Copy(serialized, sizeof(int), bitArrayBytes, 0, serialized.Length - sizeof(int));
            _hashBits = new BitArray(bitArrayBytes);
        }

        /// <summary>
        ///     Creates a new Bloom filter with error rate 1/<paramref name="capacity"/>.
        /// </summary>
        /// <param name="capacity">
        ///     The anticipated number of items to be added to the filter. More than this 
        ///     number of items can be added, but the error rate will exceed what is 
        ///     expected.
        /// </param>
        public Filter(int capacity)
        {
            int m = BestM(capacity, BestErrorRate(capacity));
            m = (m / 8) * 8; // for ease of serialization.
     
            int k = BestK(capacity, BestErrorRate(capacity));

            if (capacity < 1)
            {
                throw new ArgumentOutOfRangeException("capacity", capacity, "capacity must be > 0");
            }

            this._hashFunctionCount = k;
            this._hashBits = new BitArray(m);
        }

        /// <summary>
        /// A function that can be used to hash input.
        /// </summary>
        /// <param name="input">The values to be hashed.</param>
        /// <returns>The resulting hash code.</returns>
        private delegate int HashFunction(string input);

        /// <summary>
        /// The ratio of false to true bits in the filter. E.g., 1 true bit in a 10 bit filter means a truthiness of 0.1.
        /// </summary>
        public double Truthiness
        {
            get
            {
                return (double)this.TrueBits() / this._hashBits.Count;
            }
        }

        /// <summary>
        /// Adds a new item to the filter. It cannot be removed.
        /// </summary>
        /// <param name="item">The item.</param>
        public void Add(string item)
        {
            // start flipping bits for each hash of item
            int primaryHash = item.GetHashCode();
            int secondaryHash = this._getHashSecondary(item);
            for (int i = 0; i < this._hashFunctionCount; i++)
            {
                int hash = this.ComputeHash(primaryHash, secondaryHash, i);
                this._hashBits[hash] = true;
            }
        }

        /// <summary>
        /// Checks for the existance of the item in the filter for a given probability.
        /// </summary>
        /// <param name="item"> The item. </param>
        /// <returns> The <see cref="bool"/>. </returns>
        public bool Contains(string item)
        {
            int primaryHash = item.GetHashCode();
            int secondaryHash = this._getHashSecondary(item);
            for (int i = 0; i < this._hashFunctionCount; i++)
            {
                int hash = this.ComputeHash(primaryHash, secondaryHash, i);
                if (this._hashBits[hash] == false)
                {
                    return false;
                }
            }

            return true;
        }

        /// <summary>
        /// The best k.
        /// </summary>
        /// <param name="capacity"> The capacity. </param>
        /// <param name="errorRate"> The error rate. </param>
        /// <returns> The <see cref="int"/>. </returns>
        private static int BestK(int capacity, float errorRate)
        {
            return (int)Math.Round(Math.Log(2.0) * BestM(capacity, errorRate) / capacity);
        }

        /// <summary>
        /// The best m.
        /// </summary>
        /// <param name="capacity"> The capacity. </param>
        /// <param name="errorRate"> The error rate. </param>
        /// <returns> The <see cref="int"/>. </returns>
        private static int BestM(int capacity, float errorRate)
        {
            return (int)Math.Ceiling(capacity * Math.Log(errorRate, (1.0 / Math.Pow(2, Math.Log(2.0)))));
        }

        /// <summary>
        ///     The best error rate.
        /// </summary>
        /// <param name="capacity">
        ///     The capacity. 
        /// </param>
        /// <returns> The <see cref="float"/>. </returns>
        private static float BestErrorRate(int capacity)
        {
            float c = (float)(1.0 / capacity);
            if (c != 0)
            {
                return c;
            }

            // default
            // http://www.cs.princeton.edu/courses/archive/spring02/cs493/lec7.pdf
            return (float)Math.Pow(0.6185, int.MaxValue / capacity);
        }

        /// <summary>
        /// Hashes a string using Bob Jenkin's "One At A Time" method from Dr. Dobbs (http://burtleburtle.net/bob/hash/doobs.html).
        /// Runtime is suggested to be 9x+9, where x = input.Length. 
        /// </summary>
        /// <param name="input">The string to hash.</param>
        /// <returns>The hashed result.</returns>
        private static int HashString(string input)
        {
            string s = input as string;
            int hash = 0;

            for (int i = 0; i < s.Length; i++)
            {
                hash += s[i];
                hash += (hash << 10);
                hash ^= (hash >> 6);
            }

            hash += (hash << 3);
            hash ^= (hash >> 11);
            hash += (hash << 15);
            return hash;
        }

        /// <summary>
        /// The true bits.
        /// </summary>
        /// <returns> The <see cref="int"/>. </returns>
        private int TrueBits()
        {
            int output = 0;
            foreach (bool bit in this._hashBits)
            {
                if (bit == true)
                {
                    output++;
                }
            }

            return output;
        }

        /// <summary>
        /// Performs Dillinger and Manolios double hashing. 
        /// </summary>
        /// <param name="primaryHash"> The primary hash. </param>
        /// <param name="secondaryHash"> The secondary hash. </param>
        /// <param name="i"> The i. </param>
        /// <returns> The <see cref="int"/>. </returns>
        private int ComputeHash(int primaryHash, int secondaryHash, int i)
        {
            int resultingHash = (primaryHash + (i * secondaryHash)) % this._hashBits.Count;
            return Math.Abs((int)resultingHash);
        }

        public byte[] Serialize()
        {
            var result = new byte[sizeof(int) + _hashBits.Count / 8];
            var hfcBytes = BitConverter.GetBytes(_hashFunctionCount);
            Array.Copy(hfcBytes, result, sizeof(int));
            _hashBits.CopyTo(result, sizeof(int));            
            return result;
        }
    }
}
