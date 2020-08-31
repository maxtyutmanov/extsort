using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using ExtSort.Common;
using ExtSort.Common.Model;
using System.Collections.Concurrent;
using System.Threading.Tasks;
using ExtSort.Sorter.IO;
using ExtSort.Sorter.Config;
using System.Threading;

namespace ExtSort.Sorter
{
    public class MergePhase
    {
        private readonly List<List<string>> _inputBatches;
        private readonly IIoManager _ioManager;
        private readonly SortConfig _config;
        private readonly string _directoryPath;
        private readonly int _maxSourceFiles;

        public int Number { get; }

        public MergePhase(IIoManager ioManager, SortConfig config, int phaseNumber, string directoryPath, int maxSourceFiles)
        {
            var sourceFilePaths = Directory
                .EnumerateFiles(directoryPath, TempFilePaths.SearchPatternForPhase(phaseNumber));

            _inputBatches = sourceFilePaths
                .GetByBatches(maxSourceFiles)
                .ToList();

            _ioManager = ioManager;
            _config = config;

            Number = phaseNumber;
            _directoryPath = directoryPath;
            _maxSourceFiles = maxSourceFiles;
        }

        public bool CanRunFinal => _inputBatches.Count == 1;

        public void RunFinal(Stream output, Encoding encoding, CancellationToken ct)
        {
            if (!CanRunFinal)
                throw new InvalidOperationException("Can't run final merge because of too many intermediate input files");

            MergeSingleBatch(_inputBatches[0], output, encoding, ct);
        }

        public MergePhase RunIntermediate(CancellationToken ct)
        {
            _inputBatches.ForEach(b => MergeSingleBatchToIntermediateOutput(b, ct));
            return new MergePhase(_ioManager, _config, Number + 1, _directoryPath, _maxSourceFiles);
        }

        private void MergeSingleBatchToIntermediateOutput(List<string> inputFilePaths, CancellationToken ct)
        {
            var batchName = Guid.NewGuid().ToString();
            var outputPath = Path.Combine(_directoryPath, $"{batchName}.{TempFilePaths.ExtensionForPhase(Number + 1)}");
            using var output = File.Open(outputPath, FileMode.Create, FileAccess.Write, FileShare.None);
            MergeSingleBatch(inputFilePaths, output, Encoding.UTF8, ct);
        }

        private void MergeSingleBatch(List<string> filePaths, Stream output, Encoding encoding, CancellationToken ct)
        {
            using var _ = Measured.Operation($"merge batch (phase {Number})");

            using var inputQueues = filePaths
                .Select(_ => new BlockingCollection<ILine>(_config.Queues.MergePhaseQueueCapacity))
                .ToDisposableList();

            var inputReaderTask = RunInputReader(filePaths, inputQueues, ct);
            MergerProc(inputQueues, output, encoding ?? Encoding.UTF8, ct);
            inputReaderTask.Wait();
            // delete intermediate files we just have merged from
            filePaths.ForEach(File.Delete);
        }

        private void MergerProc(List<BlockingCollection<ILine>> inputs, Stream output, Encoding encoding, CancellationToken ct)
        {
            using var writer = _ioManager.CreateWriterForMergePhaseWrite(output, encoding);
            var inputEnumerables = inputs.Select(q => q.GetConsumingEnumerable(ct)).ToList();

            foreach (var line in KWayMerge<ILine>.Execute(inputEnumerables, ComparisonUtils.CompareLines))
            {
                ct.ThrowIfCancellationRequested();
                writer.WriteLine(line);
            }
        }

        private Task RunInputReader(IEnumerable<string> filePaths, List<BlockingCollection<ILine>> outputs, CancellationToken ct)
        {
            return Task.Run(() => InputReaderProc(filePaths, outputs, ct));
        }

        private void InputReaderProc(IEnumerable<string> filePaths, List<BlockingCollection<ILine>> outputs, CancellationToken ct)
        {
            DisposableList<IEnumerator<ILine>> lineStreams = null;
            List<ReadFilePipe> pipes = null;

            try
            {
                lineStreams = filePaths
                    .Select(path => ReadLinesFromFile(path, ct).GetEnumerator())
                    .ToDisposableList();

                pipes = lineStreams.Zip(outputs).Select(x => new ReadFilePipe(x.First, x.Second)).ToList();

                while (true)
                {
                    var hasOpenPipes = false;
                    foreach (var pipe in pipes.Where(p => p.HasMore))
                    {
                        hasOpenPipes = true;
                        pipe.TryPump();
                    }

                    if (!hasOpenPipes)
                        break;
                }
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
            }
            finally
            {
                // the avoid blocking consumer forever
                pipes?.Select(p => p.Output).Where(o => !o.IsAddingCompleted).ToList().ForEach(o => o.CompleteAdding());
                lineStreams?.Dispose();
            }
        }

        private IEnumerable<ILine> ReadLinesFromFile(string filePath, CancellationToken ct)
        {
            using var file = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.None);
            using var reader = _ioManager.CreateReaderForMergePhaseRead(file);

            while (!reader.EndOfStream)
            {
                ct.ThrowIfCancellationRequested();
                yield return reader.ReadLine();
            }
        }

        private class ReadFilePipe
        {
            private bool _hasMore;

            public IEnumerator<ILine> Input { get; }

            public BlockingCollection<ILine> Output { get; }

            public bool HasMore
            {
                get => _hasMore;
                set
                {
                    _hasMore = value;
                    if (!_hasMore && !Output.IsAddingCompleted)
                    {
                        Output.CompleteAdding();
                    }
                }
            }

            public ReadFilePipe(IEnumerator<ILine> input, BlockingCollection<ILine> output)
            {
                Input = input;
                Output = output;
                HasMore = input.MoveNext();
            }

            public void TryPump()
            {
                if (!HasMore)
                    return;

                var added = Output.TryAdd(Input.Current);
                if (added)
                {
                    HasMore = Input.MoveNext();
                }
            }
        }
    }
}
