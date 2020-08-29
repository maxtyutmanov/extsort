using System;

namespace ExtSort.Common
{
    public static class MemorySizeExt
    {
        public static long Gb(this int size) => size * 1024.Mb();

        public static long Mb(this int size) => size * 1024.Kb();

        public static long Kb(this int size) => size * 1024;
    }
}
