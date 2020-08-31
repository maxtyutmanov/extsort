using ExtSort.Common.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace ExtSort.Sorter
{
    public interface ILineWriter : IDisposable
    {
        void WriteLine(ILine line);
        void Flush();
    }
}
