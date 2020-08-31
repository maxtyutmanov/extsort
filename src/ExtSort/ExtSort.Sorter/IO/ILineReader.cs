using ExtSort.Common.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace ExtSort.Sorter
{
    public interface ILineReader : IDisposable
    {
        ILine ReadLine();
        long Position { get; }
        bool EndOfStream { get; }
        Encoding InputEncoding { get; }
    }
}
