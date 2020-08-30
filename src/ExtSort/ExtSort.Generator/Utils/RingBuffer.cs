using System;
using System.Collections.Generic;
using System.Text;

namespace ExtSort.Generator.Utils
{
    public class RingBuffer<T>
    {
        private readonly T[] _store;
        private int _count;
        private int _nextIx;

        public int Count => _count;

        public RingBuffer(int capacity)
        {
            _store = new T[capacity];
        }

        public void Add(T item)
        {
            if (_nextIx == _store.Length)
            {
                _nextIx = 0;
            }

            _store[_nextIx] = item;
            ++_nextIx;
            _count = Math.Min(_store.Length, _count + 1);
        }

        public T this[int i]
        {
            get
            {
                if (i >= Count) 
                    throw new IndexOutOfRangeException($"Items count is {Count}, requested index is {i}");

                return _store[i];
            }
        }
    }
}
