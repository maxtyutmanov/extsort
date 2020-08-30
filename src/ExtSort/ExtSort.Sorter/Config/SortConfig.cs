using ExtSort.Common;
using System;

namespace ExtSort.Sorter.Config
{
    public class SortConfig
    {
        public int MaxFilesToMerge { get; set; } = 15;

        public long InMemorySortedChunkBytes { get; set; } = 64.Mb();

        public int InputFileBufferBytes { get; set; } = (int)32.Mb();

        public int InMemorySorterThreadsCount { get; set; } = Environment.ProcessorCount;
    }
}
