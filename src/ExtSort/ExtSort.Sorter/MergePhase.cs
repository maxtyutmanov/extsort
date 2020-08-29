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
        private readonly int _phaseNumber;
        private readonly string _directoryPath;
        private readonly int _maxSourceFiles;

        public MergePhase(int phaseNumber, string directoryPath, int maxSourceFiles)
        {
            _inputBatches = Directory
                .EnumerateFiles(directoryPath, TempFilePaths.SearchPatterForPhase(phaseNumber))
                .GetByBatches(maxSourceFiles)
                .ToList();
            _phaseNumber = phaseNumber;
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
            var batchIx = 1;
            foreach (var inputBatch in _inputBatches)
            {
                var outputPath = Path.Combine(_directoryPath, $"{batchIx}.{TempFilePaths.ExtensionForPhase(_phaseNumber + 1)}");
                using var output = File.Open(outputPath, FileMode.Create, FileAccess.Write, FileShare.None);
                MergeSingleBatch(inputBatch, output);
                ++batchIx;
            }

            return new MergePhase(_phaseNumber + 1, _directoryPath, _maxSourceFiles);
        }

        private void MergeSingleBatch(List<string> filePaths, Stream output, Encoding encoding = null)
        {
            var sortedTmpEnumerables = filePaths
                .Select(ReadLinesFromFile)
                .ToArray();

            using var op = Measured.Operation($"merge batch (phase {_phaseNumber})");
            using var writer = new StreamWriter(output, encoding ?? Encoding.UTF8, bufferSize: 10_000_000, leaveOpen: true);
            foreach (var line in KWayMerge<string>.Execute(sortedTmpEnumerables, ComparisonUtils.CompareFileLines))
            {
                writer.WriteLine(line);
            }

            filePaths.ForEach(File.Delete);
        }

        private static IEnumerable<string> ReadLinesFromFile(string filePath)
        {
            using var file = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.None);
            using var reader = new StreamReader(file, bufferSize: 10_000_000);

            while (!reader.EndOfStream)
            {
                yield return reader.ReadLine();
            }
        }
    }
}
