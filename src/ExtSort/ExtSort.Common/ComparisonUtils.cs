using System;

namespace ExtSort.Common
{
    public static class ComparisonUtils
    {
        public static int CompareFileLines(string line1, string line2)
        {
            var dotIx1 = line1.IndexOf('.');
            var dotIx2 = line2.IndexOf('.');

            //// TODO: we can save some GC work by not using substring
            //var str1 = line1.Substring(dotIx1 + 2);
            //var str2 = line2.Substring(dotIx2 + 2);

            var strCompareResult = string.Compare(
                line1, dotIx1 + 2, line2, dotIx2 + 2, Math.Max(line1.Length, line2.Length), StringComparison.OrdinalIgnoreCase);

            if (strCompareResult != 0)
            {
                return strCompareResult;
            }

            var number1 = ReadNumber(line1, dotIx1);
            var number2 = ReadNumber(line2, dotIx2);

            return number1.CompareTo(number2);
        }

        private static int ReadNumber(string line, int numberLength)
        {
            var minus = line.StartsWith("-");
            var numberStart = minus ? 1 : 0;
            var number = 0;
            var multiplier = 1;
            for (var i = numberLength - 1; i >= numberStart; i--)
            {
                var digit = line[i] - '0';
                number += digit * multiplier;
                multiplier *= 10;
            }

            if (minus)
                number *= -1;

            return number;
        }
    }
}
