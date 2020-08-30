using System;
using System.Collections.Generic;
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
            {
                Console.WriteLine("E2E test needs directory in which to create test/temp files. Please specify it.");
                return -1;
            }

            var testDir = args[0];
            Directory.CreateDirectory(testDir);
            var generatedFilePath = Path.Combine(testDir, "gen.txt");
            var generator = new FileGenerator();
            generator.Run(1.Gb(), generatedFilePath);

            var initialFileHash = GetFileHashCode(generatedFilePath);

            var extSort = new ExternalSort();
            extSort.Run(generatedFilePath);
            var ctx = CheckFile(generatedFilePath);
            if (ctx.HasErrors)
            {
                Console.WriteLine("There are errors in the sorted file:");
                ctx.Errors.ForEach(Console.Error.WriteLine);
                return -1;
            }

            if (ctx.FileHashCode != initialFileHash)
            {
                Console.Error.WriteLine("Checksums for initial file and sorted files do not match");
                return -1;
            }

            Console.WriteLine("Generated file has {0} string duplicates", ctx.StringDuplicatesCount);

            Console.WriteLine("Everything is OK!");
            return 0;
        }

        static ValidationContext CheckFile(string filePath)
        {
            var ctx = new ValidationContext();

            using var reader = new StreamReader(filePath);
            while (!reader.EndOfStream)
            {
                ProcessLine(reader.ReadLine(), ctx);
                if (ctx.HasErrors)
                    break;
            }

            return ctx;
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

        private static ValidationContext ProcessLine(string line, ValidationContext ctx)
        {
            var (number, str) = ParseLine(line, ctx);
            if (ctx.HasErrors)
                return ctx;

            ctx.FileHashCode ^= line.GetHashCode();

            if (ctx.PrevString != null)
            {
                var cmpResult = string.Compare(ctx.PrevString, str, StringComparison.OrdinalIgnoreCase);
                if (cmpResult == 0)
                {
                    ctx.StringDuplicatesCount++;
                    cmpResult = ctx.PrevNumber - number;
                }

                if (cmpResult > 0)
                {
                    ctx.Errors.Add($"Line '{line}' is out of order");
                }
            }

            ctx.PrevNumber = number;
            ctx.PrevString = str;

            return ctx;
        }

        private static (int number, string str) ParseLine(string line, ValidationContext ctx)
        {
            var dotIx = line.IndexOf('.');
            if (dotIx < 0)
            {
                ctx.Errors.Add($"Line '{line}' does not have dot in it");
                return (0, null);
            }

            if (!int.TryParse(line.Substring(0, dotIx), out var number))
            {
                ctx.Errors.Add($"Line '{line}' should start with a number");
                return (0, null);
            }

            if (line.Length < dotIx + 2)
            {
                ctx.Errors.Add($"The string part is missing in line '{line}'");
                return (0, null);
            }

            // skipping the space character
            var str = line.Substring(dotIx + 2);
            return (number, str);
        }

        private class ValidationContext
        {
            public bool HasErrors => Errors.Count > 0;

            public List<string> Errors { get; } = new List<string>();

            public int FileHashCode { get; set; }

            public int StringDuplicatesCount { get; set; }

            public int PrevNumber { get; set; }

            public string PrevString { get; set; }
        }
    }
}
