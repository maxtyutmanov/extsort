using ExtSort.Common;
using System;

namespace ExtSort.Sorter.Config
{
    public class SortConfig
    {
        public int MaxFilesToMerge { get; set; } = 85;

        public long InMemorySortedChunkBytes { get; set; } = 128.Mb();

        public int InMemorySorterThreadsCount { get; set; } = 1;

        public FileBuffersConfig FileBuffers { get; set; } = new FileBuffersConfig();

        public QueuesConfig Queues { get; set; } = new QueuesConfig();
    }

    public class FileBuffersConfig
    {
        public long InitialSortInputFileBufferSize { get; set; } = 32.Mb();

        public long MergePhaseInputFileBufferSize { get; set; } = 2.Mb();

        public long InitialSortOutputFileBufferSize { get; set; } = 4.Mb();

        public long MergePhaseOutputFileBufferSize { get; set; } = 32.Mb();
    }

    public class QueuesConfig
    {
        public int MergePhaseQueueCapacity { get; set; } = 128;
    }
}
