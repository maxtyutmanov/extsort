using ExtSort.Common;
using ExtSort.Sorter.Config;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using System.Threading;
using ExtSort.Common.Model;
using System.Text;
using ExtSort.Sorter.IO;
using ExtSort.Sorter.Utils;

namespace ExtSort.Sorter
{
    public class ChunkedSortPhase
    {
        private readonly SortConfig _config;
        private readonly IIoManager _ioManager;

        public ChunkedSortPhase(SortConfig config, IIoManager ioManager)
        {
            _config = config;
            _ioManager = ioManager;
        }

        public void Run(ILineReader reader, string tempDirPath, CancellationToken ct)
        {
            // the pipeline:
            // 1. current thread reads the input stream chunk by chunk, sends non-sorted chunks to the first queue
            // 2. background sorter threads grab non-sorted chunks from the first queue, sort them and put to the second queue
            // 3. flusher thread gets sorted chunks from the second queue and flushes them to temp files

            using var nonSortedChunksQueue = new MeasuredQueue<List<ILine>>("non_sorted_chunks", _config.InMemorySorterThreadsCount);
            using var sortedChunksQueue = new MeasuredQueue<IReadOnlyList<ILine>>("sorted_chunks", _config.InMemorySorterThreadsCount);

            var sorters = RunInMemorySorters(nonSortedChunksQueue, sortedChunksQueue, ct);
            // signal the flusher thread that there will be no more sorted buffers
            Task.WhenAll(sorters).ContinueWith(_ => sortedChunksQueue.CompleteAdding());
            var flusher = RunFlusher(tempDirPath, sortedChunksQueue, ct);
            
            // read everything, wait for sorters to sort chunks and for flusher to flush them to tmp files
            ReadInput(reader, nonSortedChunksQueue, ct);
            Task.WaitAll(sorters.Concat(new[] { flusher }).ToArray());
        }

        private void ReadInput(ILineReader reader, MeasuredQueue<List<ILine>> output, CancellationToken ct)
        {
            var buffer = new List<ILine>();
            var lastFlushedPosition = reader.Position;

            while (!reader.EndOfStream)
            {
                ct.ThrowIfCancellationRequested();

                var line = reader.ReadLine();
                buffer.Add(line);

                if (reader.Position - lastFlushedPosition >= _config.InMemorySortedChunkBytes)
                {
                    var newBuffer = new List<ILine>(buffer.Capacity);
                    output.Add(buffer, ct);
                    buffer = newBuffer;
                    lastFlushedPosition = reader.Position;
                }
            }

            if (buffer.Count > 0)
                output.Add(buffer);

            output.CompleteAdding();
        }

        private Task[] RunInMemorySorters(
            MeasuredQueue<List<ILine>> input,
            MeasuredQueue<IReadOnlyList<ILine>> output,
            CancellationToken ct)
        {
            return Enumerable.Range(1, _config.InMemorySorterThreadsCount)
                .Select(_ => Task.Run(() => InMemorySortProc(input, output, ct)))
                .ToArray();
        }

        private Task RunFlusher(string tempDirPath, MeasuredQueue<IReadOnlyList<ILine>> input, CancellationToken ct)
        {
            return Task.Run(() => FlushProc(tempDirPath, input, ct));
        }

        private static void InMemorySortProc(
            MeasuredQueue<List<ILine>> input,
            MeasuredQueue<IReadOnlyList<ILine>> output,
            CancellationToken ct)
        {
            try
            {
                foreach (var buffer in input.GetConsumingEnumerable(ct))
                {
                    ct.ThrowIfCancellationRequested();
                    using var sortOp = Measured.Operation("in-memory sort");
                    buffer.Sort(ComparisonUtils.CompareLines);
                    output.Add(buffer);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
            }
        }

        private void FlushProc(
            string tempDirPath,
            MeasuredQueue<IReadOnlyList<ILine>> input,
            CancellationToken ct)
        {
            try
            {
                foreach (var sortedBuffer in input.GetConsumingEnumerable(ct))
                {
                    FlushOnce(tempDirPath, sortedBuffer, ct);
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
            }
        }

        private void FlushOnce(string tempDirPath, IReadOnlyList<ILine> sortedBuffer, CancellationToken ct)
        {
            using var flushOp = Measured.Operation("flush to tmp file");

            var sortFilePath = Path.Combine(tempDirPath, $"{Guid.NewGuid()}.{TempFilePaths.ExtensionForPhase(1)}");
            using var tmpFile = new FileStream(sortFilePath, FileMode.Create, FileAccess.Write, FileShare.None);
            using var writer = CreateLineWriter(tmpFile);

            foreach (var line in sortedBuffer)
            {
                ct.ThrowIfCancellationRequested();

                writer.WriteLine(line);
            }
        }

        private ILineWriter CreateLineWriter(Stream output)
        {
            return _ioManager.CreateWriterForInitialSortPhaseWrite(output, Encoding.UTF8);
        }
    }
}
