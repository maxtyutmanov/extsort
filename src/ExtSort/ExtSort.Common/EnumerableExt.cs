using System;
using System.Collections.Generic;
using System.Linq;
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

        public static DisposableList<T> ToDisposableList<T>(this IEnumerable<T> source)
            where T: IDisposable
        {
            var dl = new DisposableList<T>();
            dl.AddRange(source);
            return dl;
        }
    }
}
