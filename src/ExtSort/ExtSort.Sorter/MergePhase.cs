using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;
using ExtSort.Common;

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

            var sortedTmpEnumerables = filePaths
                .Select(ReadLinesFromFile)
                .ToArray();

            using var writer = new StreamWriter(output, encoding ?? Encoding.UTF8, (int)64.Mb(), leaveOpen: true);
            foreach (var line in KWayMerge<string>.Execute(sortedTmpEnumerables, ComparisonUtils.CompareFileLines))
            {
                writer.WriteLine(line);
            }

            filePaths.ForEach(File.Delete);
            
        }

        private static IEnumerable<string> ReadLinesFromFile(string filePath)
        {
            using var file = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.None);
            using var reader = new StreamReader(file, bufferSize: (int)16.Mb());

            while (!reader.EndOfStream)
            {
                yield return reader.ReadLine();
            }
        }
    }
}
