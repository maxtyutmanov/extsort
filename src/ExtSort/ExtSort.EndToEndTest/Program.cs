using System;
using System.IO;
using ExtSort.Common;
using ExtSort.Generator;
using ExtSort.Sorter;

namespace ExtSort.EndToEndTest
{
    class Program
    {
        static int Main(string[] args)
        {
            if (args.Length == 0)
                Console.WriteLine("E2E test needs directory in which to create test/temp files. Please specify it.");

            var testDir = args[0];
            Directory.CreateDirectory(testDir);
            var generatedFilePath = Path.Combine(testDir, "gen.txt");
            var generator = new FileGenerator();
            generator.Run(1.Gb(), generatedFilePath);

            var initialFileHash = GetFileHashCode(generatedFilePath);

            var extSort = new ExternalSort();
            extSort.Run(generatedFilePath);

            try
            {
                CheckTheFileIsSorted(generatedFilePath);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: {0}", ex.Message);
                return -1;
            }

            // check integrity
            var sortedFileHash = GetFileHashCode(generatedFilePath);
            if (initialFileHash != sortedFileHash)
            {
                Console.WriteLine("File hashes are different");
                return -1;
            }

            Console.WriteLine("Everything is OK!");
            return 0;
        }

        static bool CheckTheFileIsSorted(string filePath)
        {
            using var reader = new StreamReader(filePath);
            string prevLine;
            if (!reader.EndOfStream)
            {
                prevLine = reader.ReadLine();
            }
            else
            {
                Console.WriteLine("File {0} is empty", filePath);
                return true;
            }

            while (!reader.EndOfStream)
            {
                var curLine = reader.ReadLine();
                if (!LinesAreInOrder(prevLine, curLine))
                {
                    Console.WriteLine("Lines {0} and {1} are not in order", prevLine, curLine);
                    return false;
                }
                prevLine = curLine;
            }

            return true;
        }

        private static int GetFileHashCode(string filePath)
        {
            var result = 0;

            using var reader = new StreamReader(filePath);
            while (!reader.EndOfStream)
            {
                var lineHc = reader.ReadLine().GetHashCode();
                result ^= lineHc;
            }

            return result;
        }

        private static bool LinesAreInOrder(string prevLine, string curLine)
        {
            try
            {
                var dotIxPrev = prevLine.IndexOf('.');
                var dotIxCur = curLine.IndexOf('.');

                var numberPrev = int.Parse(prevLine.Substring(0, dotIxPrev));
                var numberCur = int.Parse(curLine.Substring(0, dotIxCur));

                var strPrev = prevLine.Substring(dotIxPrev + 1);
                var strCur = curLine.Substring(dotIxCur + 1);

                var cmpResult = string.Compare(strPrev, strCur, StringComparison.OrdinalIgnoreCase);
                if (cmpResult != 0)
                    return (cmpResult <= 0);

                return numberPrev <= numberCur;
            }
            catch (Exception ex)
            {
                throw new Exception($"Error when parsing lines {prevLine} and {curLine}: {ex.Message}", ex);
            }
        }
    }
}
