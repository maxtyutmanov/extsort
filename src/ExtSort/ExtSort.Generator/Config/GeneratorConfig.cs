using System;
using System.Collections.Generic;
using System.Text;

namespace ExtSort.Generator.Config
{
    public class GeneratorConfig
    {
        public int InMemoryGeneratorThreadsCount { get; set; } = Environment.ProcessorCount;

        public int MinStringLength { get; set; } = 200;

        public int MaxStringLength { get; set; } = 300;

        public double DuplicatesProbability { get; set; } = 0.1;
    }
}
