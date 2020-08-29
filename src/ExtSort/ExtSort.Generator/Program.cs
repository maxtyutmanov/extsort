using System;
using ExtSort.Common;

namespace ExtSort.Generator
{
    class Program
    {
        static void Main(string[] args)
        {
            Gen.Run(10.Gb(), @"F:\Work\ExtSortTest\out.txt");
        }
    }
}
