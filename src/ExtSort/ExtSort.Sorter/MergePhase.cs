using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using ExtSort.Common;
using ExtSort.Common.Model;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace ExtSort.Sorter
{
    public class MergePhase
    {
        private readonly List<List<string>> _inputBatches;
        private readonly string _directoryPath;
        private readonly int _maxSourceFiles;

        public int Number { get; }

        public MergePhase(int phaseNumber, string directoryPath, int maxSourceFiles)
        {
            var sourceFilePaths = Directory
                .EnumerateFiles(directoryPath, TempFilePaths.SearchPatternForPhase(phaseNumber));

            _inputBatches = sourceFilePaths
                .GetByBatches(maxSourceFiles)
                .ToList();

            Number = phaseNumber;
            _directoryPath = directoryPath;
            _maxSourceFiles = maxSourceFiles;
        }

        public bool CanRunFinal => _inputBatches.Count == 1;

        public void RunFinal(Stream output, Encoding encoding)
        {
            if (!CanRunFinal)
                throw new InvalidOperationException("Can't run final merge because of too many intermediate input files");

            MergeSingleBatch(_inputBatches[0], output, encoding);
        }

        public MergePhase RunIntermediate()
        {
            _inputBatches.ForEach(MergeSingleBatchToIntermediateOutput);
            return new MergePhase(Number + 1, _directoryPath, _maxSourceFiles);
        }

        private void MergeSingleBatchToIntermediateOutput(List<string> inputFilePaths)
        {
            var batchName = Guid.NewGuid().ToString();
            var outputPath = Path.Combine(_directoryPath, $"{batchName}.{TempFilePaths.ExtensionForPhase(Number + 1)}");
            using var output = File.Open(outputPath, FileMode.Create, FileAccess.Write, FileShare.None);
            MergeSingleBatch(inputFilePaths, output);
        }

        private void MergeSingleBatch(List<string> filePaths, Stream output, Encoding encoding = null)
        {
            using var _ = Measured.Operation($"merge batch (phase {Number})");

            using var inputQueues = filePaths.Select(_ => new BlockingCollection<ILine>(10_000)).ToDisposableList();
            var inputReaderTask = RunInputReader(filePaths, inputQueues);
            MergerProc(inputQueues, output);
            inputReaderTask.Wait();
            filePaths.ForEach(File.Delete);
        }

        private void MergerProc(List<BlockingCollection<ILine>> inputs, Stream output)
        {
            using var writer = new FastWriter(output, (int)16.Mb());
            var inputEnumerables = inputs.Select(q => q.GetConsumingEnumerable()).ToList();

            foreach (var line in KWayMerge<ILine>.Execute(inputEnumerables, ComparisonUtils.CompareLines))
            {
                writer.WriteLine(line);
            }
        }

        private Task RunInputReader(IEnumerable<string> filePaths, List<BlockingCollection<ILine>> outputs)
        {
            return Task.Run(() => InputReaderProc(filePaths, outputs));
        }

        private void InputReaderProc(IEnumerable<string> filePaths, List<BlockingCollection<ILine>> outputs)
        {
            try
            {
                using var lineStreams = filePaths.Select(ReadLinesFromFile).Select(e => e.GetEnumerator()).ToDisposableList();

                var activeStreamsCount = lineStreams.Count;

                while (activeStreamsCount > 0)
                {
                    for (var i = 0; i < lineStreams.Count; i++)
                    {
                        if (lineStreams[i].MoveNext())
                        {
                            outputs[i].TryAdd(lineStreams[i].Current, 1);
                        }
                        else if (!outputs[i].IsAddingCompleted)
                        {
                            outputs[i].CompleteAdding();
                            activeStreamsCount--;
                        }
                    }
                }
            }
            finally
            {
                outputs.Where(o => !o.IsAddingCompleted).ToList().ForEach(o => o.CompleteAdding());
            }
        }

        private static IEnumerable<ILine> ReadLinesFromFile(string filePath)
        {
            using var file = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.None);
            using var reader = new FastReader(file, (int)8.Mb());

            while (!reader.EndOfStream)
            {
                yield return reader.ReadLine();
            }
        }
    }
}
