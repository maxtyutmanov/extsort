using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ExtSort.Common.Model
{
    public interface ILine : IComparable
    {
        void WriteToStream(Stream stream, byte[] digitBuffer);
    }
}
