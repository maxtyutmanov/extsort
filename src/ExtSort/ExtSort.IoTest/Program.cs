using ExtSort.Common;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace ExtSort.IoTest
{
    class Program
    {
        static void Main(string[] args)
        {
            // how fast can we copy a single file?

            // unbounded PC queue
            var queue = new BlockingCollection<string>(2_000_000);

            var srcFilePath = @"F:\Work\ExtSortTest\gen.txt";
            var trgFilePath = @"F:\Work\ExtSortTest\gen_copy.txt";

            var writerTask = Task.Run(() => WriteToTrgFile(trgFilePath, queue));

            using var srcFile = File.Open(srcFilePath, FileMode.Open, FileAccess.Read, FileShare.None);
            
            using var reader = new StreamReader(srcFile);
            
            using var _ = Measured.Operation("copy single file");
            while (!reader.EndOfStream)
            {
                var line = reader.ReadLine();
                queue.Add(line);
            }

            queue.CompleteAdding();

            writerTask.Wait();
        }

        private static void WriteToTrgFile(string trgFilePath, BlockingCollection<string> queue)
        {
            using var trgFile = File.Open(trgFilePath, FileMode.Create, FileAccess.Write, FileShare.None);
            using var writer = new StreamWriter(trgFile);
            var bufferCapacity = 1_000_000;
            var buffer = new List<string>(bufferCapacity);

            foreach (var line in queue.GetConsumingEnumerable())
            {
                buffer.Add(line);
                if (buffer.Count == bufferCapacity)
                {
                    buffer.ForEach(l => writer.WriteLine(l));
                    buffer.Clear();
                }
            }

            if (buffer.Count != 0)
            {
                buffer.ForEach(l => writer.WriteLine(l));
            }
        }
    }
}
