using System;

namespace ExtSort.Common.Model
{
    public class Line : ILine
    {
        public int Number { get; }
        public string Str { get; }

        public Line(int number, string str)
        {
            Number = number;
            Str = str;
        }

        public int CompareTo(object obj)
        {
            var other = (Line)obj;

            var cmpResult = string.Compare(Str, other.Str, StringComparison.OrdinalIgnoreCase);
            if (cmpResult != 0)
                return cmpResult;

            return Number - other.Number;
        }
    }
}
