using ExtSort.Common;
using ExtSort.Generator.Config;
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
    public class FileGenerator
    {
        private readonly GeneratorConfig _config;
        private readonly List<ILineGenerator> _lineGenerators;

        public FileGenerator(GeneratorConfig config, Func<ILineGenerator> lineGeneratorFactory)
        {
            _config = config;
            _lineGenerators = Enumerable.Range(0, _config.InMemoryGeneratorThreadsCount)
                .Select(_ => lineGeneratorFactory())
                .ToList();
        }

        public FileGenerator(GeneratorConfig config)
            : this(config, () => new LineGenerator(config))
        {
        }

        public FileGenerator()
            : this(new GeneratorConfig())
        {
        }

        public void Run(long bytesToGenerate, string outFilePath, CancellationToken ct = default)
        {
            using var file = File.Open(outFilePath, FileMode.Create, FileAccess.Write, FileShare.None);
            Run(bytesToGenerate, file, ct);
        }

        public void Run(long bytesToGenerate, Stream outStream, CancellationToken ct = default)
        {
            using var _ = Measured.Operation("generate test file");

            var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

            using var generatedLinesQueue = new BlockingCollection<string>(1_000_000);
            var generatorTasks = _lineGenerators
                .Select(lg => Task.Run(() => RunLineGenerator(lg, generatedLinesQueue, cts.Token)))
                .ToArray();

            Console.WriteLine("Created {0} generator threads", generatorTasks.Length);

            // using large output buffer
            using var writer = new StreamWriter(outStream, Encoding.UTF8, (int)1.Mb(), true);
            foreach (var line in generatedLinesQueue.GetConsumingEnumerable())
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

            Task.WaitAll(generatorTasks);
        }

        private void RunLineGenerator(ILineGenerator lg, BlockingCollection<string> output, CancellationToken ct)
        {
            while (!ct.IsCancellationRequested)
            {
                output.Add(lg.Next());
            }
        }
    }
}
