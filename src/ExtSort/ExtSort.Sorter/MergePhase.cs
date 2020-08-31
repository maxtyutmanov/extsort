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

            //var sortedTmpEnumerables = filePaths
            //    .Select(ReadLinesFromFile)
            //    .ToArray();

            var inputQueues = filePaths.Select(_ => new BlockingCollection<ILine>(100_000)).ToList();
            var mergedOutputQueue = new BlockingCollection<ILine>(1_000_000);
            var inputReaderTask = RunInputReader(filePaths, inputQueues);
            var mergerTask = RunMerger(inputQueues, mergedOutputQueue);

            using var writer = new FastWriter(output, (int)128.Mb());
            foreach (var line in mergedOutputQueue.GetConsumingEnumerable())
            {
                writer.WriteLine(line);
            }

            //using var writer = new StreamWriter(output, encoding ?? Encoding.UTF8, (int)64.Mb(), leaveOpen: true);
            //using var writer = new FastWriter(output, (int)64.Mb());
            //foreach (var line in KWayMerge<ILine>.Execute(inputEnumerables, ComparisonUtils.CompareLines))
            //{
            //    writer.WriteLine(line);
            //}

            filePaths.ForEach(File.Delete);
        }

        private Task RunMerger(List<BlockingCollection<ILine>> inputs, BlockingCollection<ILine> output)
        {
            return Task.Run(() => MergerProc(inputs, output));
        }

        private void MergerProc(List<BlockingCollection<ILine>> inputs, BlockingCollection<ILine> output)
        {
            var inputEnumerables = inputs.Select(q => q.GetConsumingEnumerable()).ToList();

            foreach (var line in KWayMerge<ILine>.Execute(inputEnumerables, ComparisonUtils.CompareLines))
            {
                output.Add(line);
            }

            output.CompleteAdding();
            Console.WriteLine("Finished merging data for phase {0}", Number);
        }

        private Task RunInputReader(IEnumerable<string> filePaths, List<BlockingCollection<ILine>> outputs)
        {
            return Task.Run(() => InputReaderProc(filePaths, outputs));
        }

        private void InputReaderProc(IEnumerable<string> filePaths, List<BlockingCollection<ILine>> outputs)
        {
            List<IEnumerator<ILine>> lineStreams = null;

            try
            {
                lineStreams = filePaths.Select(ReadLinesFromFile).Select(e => e.GetEnumerator()).ToList();
                var activeStreamsCount = lineStreams.Count;

                while (activeStreamsCount > 0)
                {
                    for (var i = 0; i < lineStreams.Count; i++)
                    {
                        if (lineStreams[i].MoveNext())
                        {
                            outputs[i].Add(lineStreams[i].Current);
                        }
                        else
                        {
                            outputs[i].CompleteAdding();
                            activeStreamsCount--;
                        }
                    }
                }
            }
            finally
            {
                Console.WriteLine("Finished reading input for phase {0}", Number);
                lineStreams?.ForEach(ls => ls.Dispose());
                outputs.Where(o => !o.IsAddingCompleted).ToList().ForEach(o => o.CompleteAdding());
            }
        }

        private static IEnumerable<ILine> ReadLinesFromFile(string filePath)
        {
            using var file = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.None);
            //using var reader = new StreamReader(file, bufferSize: (int)16.Mb());
            using var reader = new FastReader(file, (int)64.Mb());

            while (!reader.EndOfStream)
            {
                yield return reader.ReadLine();
            }
        }
    }
}
