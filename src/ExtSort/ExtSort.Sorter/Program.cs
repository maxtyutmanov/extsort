using System;
using System.IO;

namespace ExtSort.Sorter
{
    class Program
    {
        static int Main(string[] args)
        {
            if (args.Length < 1)
            {
                Console.WriteLine("Specify full path to the file that needs to be sorted");
                Console.ReadLine();
                return -1;
            }

            if (!File.Exists(args[0]))
            {
                Console.WriteLine("File {0} does not exist", args[0]);
                Console.ReadLine();
                return -1;
            }
            
            var extSort = new ExternalSort();
            extSort.Run(args[0]);
            return 0;
        }
    }
}
