using ExtSort.Common;
using System;

namespace ExtSort.Sorter.Config
{
    public class SortConfig
    {
        public int MaxFilesToMerge { get; set; } = 40;

        public long InMemorySortedChunkBytes { get; set; } = 256.Mb();

        public int InputFileBufferBytes { get; set; } = (int)32.Mb();

        public int InMemorySorterThreadsCount { get; set; } = 2;
    }
}
