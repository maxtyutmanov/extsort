using System;
using System.Collections.Generic;
using System.Text;

namespace ExtSort.Common
{
    public static class EnumerableExt
    {
        public static IEnumerable<List<T>> GetByBatches<T>(this IEnumerable<T> source, int batchSize)
        {
            var batch = new List<T>();
            var en = source.GetEnumerator();

            while (en.MoveNext())
            {
                batch.Add(en.Current);
                if (batch.Count == batchSize)
                {
                    yield return batch;
                    batch = new List<T>();
                }
            }

            if (batch.Count != 0)
                yield return batch;
        }
    }
}
