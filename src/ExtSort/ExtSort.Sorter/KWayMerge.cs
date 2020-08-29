using System;
using System.Collections.Generic;
using System.Linq;

namespace ExtSort.Sorter
{
    public class KWayMerge<T> : IDisposable
    {
        private readonly List<IEnumerator<T>> _sortedEnumerators;
        private readonly IReadOnlyList<IEnumerator<T>> _initialEnumerators;
        private readonly Comparison<T> _comparison;

        private KWayMerge(IEnumerator<T>[] sortedEnumerators, Comparison<T> comparison)
        {
            if (sortedEnumerators == null) throw new ArgumentNullException(nameof(sortedEnumerators));
            if (sortedEnumerators.Length == 0)
                throw new ArgumentException("Value cannot be an empty collection.", nameof(sortedEnumerators));

            _sortedEnumerators = sortedEnumerators
                .Where(en => en.MoveNext()) // only non-empty enumerators
                .ToList();
            _initialEnumerators = new List<IEnumerator<T>>(sortedEnumerators);
            _comparison = comparison;

            BuildHeap();
        }

        public void Dispose()
        {
            foreach (var en in _initialEnumerators)
            {
                en.Dispose();
            }
        }

        public static IEnumerable<T> Execute(IEnumerable<T>[] sortedEnumerables, Comparison<T> comparison)
        {
            var enumerators = sortedEnumerables.Select(e => e.GetEnumerator()).ToArray();

            using var kWayMerge = new KWayMerge<T>(enumerators, comparison);
            foreach (var item in kWayMerge.Execute())
            {
                yield return item;
            }
        }
        
        private IEnumerable<T> Execute()
        {
            while (_sortedEnumerators.Count != 0)
            {
                // at position 0 there's always a maximum/minimum item thanks to the heap's main property
                var topEnumerator = _sortedEnumerators[0];

                yield return topEnumerator.Current;

                if (topEnumerator.MoveNext())
                {
                    Heapify(0);
                }
                else
                {
                    _sortedEnumerators.Remove(topEnumerator);
                    // rebuild heap, because we have deleted an item from it
                    BuildHeap();
                }
            }
        }

        private void BuildHeap()
        {
            for (var i = _sortedEnumerators.Count / 2; i >= 0; i--)
            {
                Heapify(i);
            }
        }

        private void Heapify(int index)
        {
            int indexOfLeft = 2 * index + 1;
            int indexOfRight = 2 * index + 2;

            // here we ensure that item located upper in the heap is smaller/greater 
            // (depends on the sorting direction) than its two children
            int indexOfUpper = index;

            if (indexOfLeft < _sortedEnumerators.Count && IsSwapped(indexOfUpper, indexOfLeft))
            {
                indexOfUpper = indexOfLeft;
            }

            if (indexOfRight < _sortedEnumerators.Count && IsSwapped(indexOfUpper, indexOfRight))
            {
                indexOfUpper = indexOfRight;
            }

            if (indexOfUpper != index)
            {
                Swap(index, indexOfUpper);
                Heapify(indexOfUpper);
            }
        }

        private void Swap(int left, int right)
        {
            var tmp = _sortedEnumerators[left];
            _sortedEnumerators[left] = _sortedEnumerators[right];
            _sortedEnumerators[right] = tmp;
        }

        private bool IsSwapped(int upper, int lower)
        {
            // We build a min heap, and element from upper level should not be greater than element from lower level.
            // if this is the case, it's swapped
            return _comparison(_sortedEnumerators[upper].Current, _sortedEnumerators[lower].Current) > 0;
        }
    }
}
