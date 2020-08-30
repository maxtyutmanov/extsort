using System;
using ExtSort.Common;

namespace ExtSort.Generator
{
    class Program
    {
        static int Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Specify path to file to generate, and size of file in GB");
                Console.ReadLine();
                return -1;
            }

            var filePath = args[0];
            var fileSize = int.Parse(args[1]).Gb();

            var generator = new FileGenerator();
            generator.Run(fileSize, filePath);
            return 0;
        }
    }
}
