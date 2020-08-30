using ExtSort.Sorter;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace ExtSort.UnitTests
{
    public class KWayMergeTests
    {
        [Fact]
        public void MergeSortedEnumerators_ResultShouldBeSorted()
        {
            var initial = GenerateRandomSortedEnumerables(10, 10, 100);
            var result = KWayMerge<int>.Execute(initial, (x1, x2) => x1 - x2).ToList();
            result.Should().Equal(initial.SelectMany(x => x).OrderBy(x => x));
        }

        private List<List<int>> GenerateRandomSortedEnumerables(int enumerablesCount, int minItemsInEnumerable, int maxItemsInEnumerable)
        {
            var rand = new Random();
            var result = new List<List<int>>();

            for (var i = 0; i < enumerablesCount; i++)
            {
                var itemsCount = rand.Next(minItemsInEnumerable, maxItemsInEnumerable + 1);
                var enumerable = new List<int>();
                for (int j = 0; j < itemsCount; j++)
                {
                    enumerable.Add(rand.Next());
                }
                enumerable.Sort();
                result.Add(enumerable);
            }

            return result;
        }
    }
}
