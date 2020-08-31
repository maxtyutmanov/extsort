using ExtSort.Common.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ExtSort.Sorter
{
    public class FastWriter : IDisposable
    {
        private readonly Stream _output;
        // 12 decimal digits should be enough for any int
        private readonly byte[] _digitBuffer = new byte[20];

        public FastWriter(Stream output, int bufferSize)
        {
            _output = new BufferedStream(output, bufferSize);
            WriteBom();
        }

        public void WriteLine(ILine line)
        {
            line.WriteToStream(_output, _digitBuffer);
        }

        public void Dispose()
        {
            // not disposing bufferedstream, because it would lead to the initial stream being disposed
            _output.Flush();
        }

        private void WriteBom()
        {
            _output.Write(Encoding.UTF8.Preamble);
        }
    }
}
