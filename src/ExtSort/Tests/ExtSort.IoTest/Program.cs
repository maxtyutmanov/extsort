using ExtSort.Common;
using ExtSort.Common.Model;
using ExtSort.Sorter;
using ExtSort.Sorter.IO;
using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Threading.Tasks;

namespace ExtSort.IoTest
{
    class Program
    {
        static void Main(string[] args)
        {
            // how fast can we copy a single file?

            var srcFilePath = @"F:\Work\ExtSortTest\gen.txt";
            var trgFilePath = @"F:\Work\ExtSortTest\gen_copy.txt";

            //RunWithCustomIO(srcFilePath, trgFilePath);
            //RunWithStandardIO(srcFilePath, trgFilePath);
            //RunWithPipelinesIO(srcFilePath, trgFilePath);
            RunWithCustomLineReader(srcFilePath, trgFilePath);
        }

        private static void RunWithCustomLineReader(string srcFilePath, string trgFilePath)
        {
            using var srcFile = new FileStream(srcFilePath, FileMode.Open, FileAccess.Read, FileShare.None);
            using var trgFile = new FileStream(trgFilePath, FileMode.Create, FileAccess.Write, FileShare.None);

            var reader = new LineReader(srcFile, (int)64.Kb());
            var sw = System.Diagnostics.Stopwatch.StartNew();
            var i = 0L;
            while (!reader.EndOfStream)
            {
                //var line = FastLine.ParseFromStream(srcFile, b); //reader.ReadLine();
                var line = reader.ReadLine();
                if (++i % 1_000_000 == 0)
                {
                    var mb = reader.Position / 1.Mb();
                    Console.WriteLine("Read {0} lines ({1} MB) in {2}!", i, mb, sw.Elapsed);
                }
            }
        }

        private static void RunWithStandardIO(string srcFilePath, string trgFilePath)
        {
            using var srcFile = new FileStream(srcFilePath, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize: (int)64.Kb());
            using var trgFile = new FileStream(trgFilePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: (int)64.Kb());
            var reader = new StreamReader(srcFile);

            var sw = System.Diagnostics.Stopwatch.StartNew();
            var i = 1L;
            //while (!reader.EndOfStream)
            //{
            //    //var line = FastLine.ParseFromStream(srcFile, b); //reader.ReadLine();
            //    var line = reader.ReadLine();
            //    if (++i % 1_000_000 == 0)
            //    {
            //        var mb = srcFile.Position / 1.Mb();
            //        Console.WriteLine("Read {0} lines ({1} MB) in {2}!", i, mb, sw.Elapsed);
            //    }
            //}
            int c;
            while ((c = reader.Read()) != -1)
            {
                if (c == '\n')
                {
                    if (++i % 1_000_000 == 0)
                    {
                        var mb = srcFile.Position / 1.Mb();
                        Console.WriteLine("Read {0} lines ({1} MB) in {2}!", i, mb, sw.Elapsed);
                    }
                }
            }
        }

        private static void RunWithPipelinesIO(string srcFilePath, string trgFilePath)
        {
            using var srcFile = new FileStream(srcFilePath, FileMode.Open, FileAccess.Read, FileShare.None, bufferSize: (int)64.Kb());
            using var trgFile = new FileStream(trgFilePath, FileMode.Create, FileAccess.Write, FileShare.None, bufferSize: (int)64.Kb());
            var pipe = new Pipe();

            var sw = System.Diagnostics.Stopwatch.StartNew();
            var i = 0L;

            var fillTask = FillPipeAsync(srcFile, pipe.Writer);
            var readTask = ReadPipeAsync(pipe.Reader, (seq) =>
            {
                if (++i % 1_000_000 == 0)
                {
                    var mb = srcFile.Position / 1.Mb();
                    Console.WriteLine("Read {0} lines ({1} MB) in {2}!", i, mb, sw.Elapsed);
                }
            });

            Task.WaitAll(fillTask, readTask);
        }

        private static async Task FillPipeAsync(FileStream stream, PipeWriter writer)
        {
            while (true)
            {
                try
                {
                    var memory = writer.GetMemory((int)2.Kb());
                    var bytesRead = await stream.ReadAsync(memory);
                    if (bytesRead == 0)
                        break;

                    writer.Advance(bytesRead);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error: {0}", ex.Message);
                    break;
                }

                var result = await writer.FlushAsync();
                if (result.IsCompleted)
                    break;
            }

            writer.Complete();
        }

        private static async Task ReadPipeAsync(PipeReader reader, Action<ReadOnlySequence<byte>> processLine)
        {
            while (true)
            {
                var result = await reader.ReadAsync();

                ReadOnlySequence<byte> buffer = result.Buffer;
                SequencePosition? position;
                do
                {
                    // Look for a EOL in the buffer
                    position = buffer.PositionOf((byte)'\n');

                    if (position != null)
                    {
                        // Process the line
                        processLine(buffer.Slice(0, position.Value));

                        // Skip the line + the \n character (basically position)
                        buffer = buffer.Slice(buffer.GetPosition(1, position.Value));
                    }
                }
                while (position != null);

                // Tell the PipeReader how much of the buffer we have consumed
                reader.AdvanceTo(buffer.Start, buffer.End);

                // Stop reading if there's no more data coming
                if (result.IsCompleted)
                    break;
            }
        }
    }
}
