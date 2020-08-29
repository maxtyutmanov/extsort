using ExtSort.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ExtSort.Generator
{
    public static class Gen
    {
        private static readonly IReadOnlyList<char> Alphabet = CreateAlphabet();
        private const int MinStrLength = 10;
        private const int MaxStrLength = 100;

        public static void Run(long bytesToGenerate, string outFilePath)
        {
            using var file = File.Open(outFilePath, FileMode.Create, FileAccess.Write, FileShare.None);
            Run(bytesToGenerate, file);
        }

        public static void Run(long bytesToGenerate, Stream outStream, CancellationToken ct = default)
        {
            using var _ = Measured.Operation("generate test file");

            var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

            using var queue = new BlockingCollection<string>(1_000_000);
            var generators = Enumerable.Range(0, Environment.ProcessorCount)
                .Select(recCount => Task.Run(() => RunLineGenerator(queue, cts.Token)))
                .ToArray();

            Console.WriteLine("Created {0} generator threads", generators.Length);

            // using large output buffer
            using var writer = new StreamWriter(outStream, Encoding.UTF8, 1_000_000, true);
            foreach (var line in queue.GetConsumingEnumerable())
            {
                if (outStream.Position >= bytesToGenerate)
                {
                    cts.Cancel();
                    break;
                }

                if (ct.IsCancellationRequested)
                {
                    break;
                }

                writer.WriteLine(line);
            }

            Task.WaitAll(generators);
        }

        private static void RunLineGenerator(BlockingCollection<string> queue, CancellationToken ct)
        {
            var rand = new Random();
            var sb = new StringBuilder();
            
            while (!ct.IsCancellationRequested)
            {
                sb.Clear();
                var number = rand.Next();
                sb.Append(number);
                sb.Append(". ");
                var strLength = rand.Next(MinStrLength, MaxStrLength + 1);
                AppendRandString(sb, rand, strLength);
                queue.Add(sb.ToString());
            }
        }

        private static void AppendRandString(StringBuilder sb, Random rand, int length)
        {
            for (var i = 0; i < length; i++)
            {
                var c = Alphabet[rand.Next(Alphabet.Count)];
                sb.Append(c);
            }
        }

        private static List<char> CreateAlphabet()
        {
            var lowercase = Enumerable
                .Range(Convert.ToInt32('a'), Convert.ToInt32('z') - Convert.ToInt32('a') + 1)
                .Select(i => Convert.ToChar(i))
                .ToList();

            var uppercase = lowercase.Select(char.ToUpperInvariant);

            return lowercase.Concat(uppercase).ToList();
        }
    }
}
