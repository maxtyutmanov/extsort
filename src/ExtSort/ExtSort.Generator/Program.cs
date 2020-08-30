using System;
using ExtSort.Common;

namespace ExtSort.Generator
{
    class Program
    {
        static void Main(string[] args)
        {
            var generator = new FileGenerator();
            generator.Run(10.Gb(), @"F:\Work\ExtSortTest\out.txt");
        }
    }
}
