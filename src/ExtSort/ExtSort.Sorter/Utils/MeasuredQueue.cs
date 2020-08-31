using ExtSort.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace ExtSort.Sorter.Utils
{
    public class MeasuredQueue<T> : IDisposable
    {
        private readonly BlockingCollection<T> _inner;
        private readonly string _name;

        public MeasuredQueue(string name, int capacity)
        {
            _inner = new BlockingCollection<T>(capacity);
            _name = name;
        }

        public void Dispose()
        {
            _inner.Dispose();
        }

        public void Add(T item, CancellationToken ct = default)
        {
            using var _ = Measured.Operation($"write wait on queue '{_name}'");
            _inner.Add(item, ct);
        }

        public void CompleteAdding()
        {
            _inner.CompleteAdding();
        }

        public IEnumerable<T> GetConsumingEnumerable(CancellationToken ct = default)
        {
            using var en = _inner.GetConsumingEnumerable(ct).GetEnumerator();
            while (MeasuredMoveNext(en))
                yield return en.Current;
        }

        private bool MeasuredMoveNext(IEnumerator<T> en)
        {
            using var _ = Measured.Operation($"read wait on queue '{_name}'");
            return en.MoveNext();
        }
    }
}
