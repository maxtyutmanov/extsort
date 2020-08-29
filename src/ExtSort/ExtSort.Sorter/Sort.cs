using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using ExtSort.Common;

namespace ExtSort.Sorter
{
    public static class Sort
    {
        private static readonly long MaxInMemoryBytes = 512.Mb();
        private static readonly int MaxFilesInMergePhase = 8;
        private static readonly int InMemorySortersCount = Environment.ProcessorCount;
        private static readonly long BytesPerSingleBuffer = MaxInMemoryBytes / InMemorySortersCount;

        public static void Run(string filePath, string tempDirPath = null)
        {
            if (tempDirPath == null)
            {
                tempDirPath = Path.GetDirectoryName(filePath);
            }

            using var file = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            Run(file, tempDirPath);
        }

        public static void Run(Stream input, string tempDirPath)
        {
            using var sortOp = Measured.Operation("entire sorting operation");

            // Not using any asynchrony, because... what for?
            // Asynchrony is good for saving threads, and we don't need many threads here.

            PrepareTempDirectory(tempDirPath);

            var initialPosition = input.Position;
            using var reader = new StreamReader(input, bufferSize: 1_000_000, leaveOpen: true);

            using (var _ = Measured.Operation("initial sort phase"))
            {
                // producer-consumer queue. Producer is the current thread (reads lines from input),
                // consumers are threads that perform in-memory sorting and flush sorted chunks
                // to tmp files
                var pcQueue = new BlockingCollection<List<string>>(InMemorySortersCount);
                var sorters = RunInMemorySorters(tempDirPath, pcQueue);

                // read everything, wait for sorters to produce tmp files (sorted chunks)
                ReadInput(reader, pcQueue);
                Task.WaitAll(sorters);
            }

            using (var _ = Measured.Operation("all merge phases"))
            {
                // Merge chunks together in multiple phases.
                // Not merge everything in a single phase, because it would lead to too many open files from which we would read in parallel,
                // IO performance will deteriorate this way.
                var mergePhase = new MergePhase(1, tempDirPath, MaxFilesInMergePhase);
                while (!mergePhase.CanRunFinal)
                {
                    mergePhase = mergePhase.RunIntermediate();
                }
                // finally, merge everything back to input file
                input.Position = initialPosition;
                mergePhase.RunFinal(input, reader.CurrentEncoding);
            }
        }

        private static void ReadInput(StreamReader reader, BlockingCollection<List<string>> output)
        {
            var buffer = new List<string>();
            var lastFlushedPosition = reader.BaseStream.Position;

            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                buffer.Add(line);

                if (reader.BaseStream.Position - lastFlushedPosition >= BytesPerSingleBuffer)
                {
                    output.Add(buffer);
                    buffer = new List<string>(buffer.Capacity);
                    lastFlushedPosition = reader.BaseStream.Position;
                }
            }

            if (buffer.Count > 0)
                output.Add(buffer);

            output.CompleteAdding();
        }

        private static void PrepareTempDirectory(string tempDirPath)
        {
            var dir = Directory.CreateDirectory(tempDirPath);
            dir.EnumerateFiles(TempFilePaths.SearchPattern).ToList().ForEach(f => f.Delete());
        }

        private static Task[] RunInMemorySorters(string tempDirPath, BlockingCollection<List<string>> input)
        {
            return Enumerable.Range(1, InMemorySortersCount)
                .Select(workerIx => Task.Run(() => RunInMemorySortWorker(workerIx, tempDirPath, input)))
                .ToArray();
        }

        private static void RunInMemorySortWorker(int workerIx, string tempDirPath, BlockingCollection<List<string>> queue)
        {
            var fileIx = 1;
            foreach (var buffer in queue.GetConsumingEnumerable())
            {
                using (var sortOp = Measured.Operation("in-memory sort"))
                {
                    buffer.Sort(ComparisonUtils.CompareFileLines);
                }

                using (var flushOp = Measured.Operation("flush to tmp file"))
                {
                    var sortFilePath = Path.Combine(tempDirPath, $"{workerIx}-{fileIx}.{TempFilePaths.ExtensionForPhase(1)}");
                    using var tmpFile = File.Open(sortFilePath, FileMode.Create, FileAccess.Write, FileShare.None);
                    using var writer = new StreamWriter(tmpFile, bufferSize: 1_000_000);
                    // TODO: this interleaves with the process of reading input file, need a centralized dispatcher for IO
                    buffer.ForEach(writer.WriteLine);
                }
                
                ++fileIx;
            }
        }
    }
}
