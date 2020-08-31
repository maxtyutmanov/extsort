using ExtSort.Common.Model;

namespace ExtSort.Common
{
    public static class ComparisonUtils
    {
        public static int CompareLines(ILine line1, ILine line2)
        {
            return line1.CompareTo(line2);
        }
    }
}
