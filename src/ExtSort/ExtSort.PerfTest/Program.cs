using ExtSort.Generator;
using ExtSort.Common;
using System;
using ExtSort.Sorter;

namespace ExtSort.PerfTest
{
    class Program
    {
        static int Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Specify path to file to generate and sort, and size of file in GB");
                return -1;
            }

            var filePath = args[0];
            var fileSize = int.Parse(args[1]).Gb();

            var generator = new FileGenerator();
            generator.Run(fileSize, filePath);
            var extSort = new ExternalSort();
            extSort.Run(filePath);

            return 0;
        }
    }
}
