using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using ExtSort.Common;
using ExtSort.Sorter.Config;
using ExtSort.Sorter.IO;

namespace ExtSort.Sorter
{
    public class ExternalSort
    {
        private readonly SortConfig _config;
        private readonly IIoManager _ioManager;

        public ExternalSort(SortConfig config = null)
        {
            _config = config ?? new SortConfig();
            _ioManager = new IoManager(_config.FileBuffers);
        }

        public void Run(string filePath, string tempDirPath = null, CancellationToken ct = default)
        {
            if (tempDirPath == null)
            {
                tempDirPath = Path.GetDirectoryName(filePath);
            }

            using var file = new FileStream(filePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            Run(file, tempDirPath, ct);
        }

        public void Run(Stream input, string tempDirPath, CancellationToken ct)
        {
            using var sortOp = Measured.Operation("entire sorting operation");
            using var reader = _ioManager.CreateReaderForInitialSortPhaseRead(input);

            // Not using any asynchrony.
            // Asynchrony is good for scaling, here we just need a couple of threads from threadpool.

            PrepareTempDirectory(tempDirPath);
            RunChunkedSort(reader, tempDirPath, ct);
            RunMiltiphaseMerge(input, reader.InputEncoding, tempDirPath, ct);
        }

        private void RunChunkedSort(ILineReader reader, string tempDirPath, CancellationToken ct)
        {
            using var _ = Measured.Operation("chunked sort phase");
            var sortPhase = new ChunkedSortPhase(_config, _ioManager);
            sortPhase.Run(reader, tempDirPath, ct);
        }

        private void RunMiltiphaseMerge(Stream input, Encoding encoding, string tempDirPath, CancellationToken ct)
        {
            using var _ = Measured.Operation("all merge phases");
            
            // Merge chunks together in multiple phases.
            // Not merge everything in a single phase, because it would lead to too many open files from which we would read in parallel,
            // IO performance will deteriorate this way.

            var initialFilesCount = Directory.EnumerateFiles(tempDirPath, TempFilePaths.SearchPatternForPhase(1)).Count();
            var optimalMaxFilesCount = CalculateOptimalMaxFilesCountToMerge(initialFilesCount);

            var mergePhase = new MergePhase(_ioManager, _config, 1, tempDirPath, optimalMaxFilesCount);
            while (!mergePhase.CanRunFinal)
            {
                using var mergePhaseOp = Measured.Operation($"Merge phase {mergePhase.Number}");
                mergePhase = mergePhase.RunIntermediate(ct);
            }
            // finally, merge everything back to input file
            input.Position = 0;
            mergePhase.RunFinal(input, encoding, ct);
        }

        private int CalculateOptimalMaxFilesCountToMerge(int initialFilesCount)
        {
            var phasesCount = 1;
            var filesCount = initialFilesCount;

            while (filesCount > _config.MaxFilesToMerge)
            {
                // too many input files, have to add one more phase
                ++phasesCount;
                filesCount = (int)Math.Ceiling(Math.Pow(initialFilesCount, 1.0 / phasesCount));
            }

            return filesCount;
        }

        private static void PrepareTempDirectory(string tempDirPath)
        {
            var dir = Directory.CreateDirectory(tempDirPath);
            dir.EnumerateFiles(TempFilePaths.SearchPattern).ToList().ForEach(f => f.Delete());
        }
    }
}
