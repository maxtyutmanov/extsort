using ExtSort.Common.Model;
using System;
using System.IO;
using System.Text;
using ExtSort.Common;
using System.Collections.Generic;
using ExtSort.Sorter.Utils;

namespace ExtSort.Sorter
{
    public class FastReader : IDisposable
    {
        private readonly List<byte> _tmpStrBuffer = new List<byte>();
        private readonly Stream _input;
        private readonly ReadonlyStream _roInput;

        public long Position => _input.Position;

        public bool EndOfStream => _input.IsEof();

        public FastReader(Stream input, int bufferSize)
        {
            if (input.Position != 0)
                throw new InvalidOperationException("Position must be 0");

            _input = new BufferedStream(input, bufferSize);
            _roInput = new ReadonlyStream(_input);
            SkipBom();
        }

        public ILine ReadLine()
        {
            var line = FastLine.ParseFromStream(_input, _tmpStrBuffer);
            return line;
        }

        public void Dispose()
        {
            // not disposing bufferedstream, because it would lead to the initial stream being disposed
            _input.Flush();
        }

        private void SkipBom()
        {
            var bom = Encoding.UTF8.Preamble;
            if (_roInput.Length < bom.Length)
                return;

            for (int i = 0; i < bom.Length; i++)
            {
                if (_roInput.ReadByte() != bom[i])
                {
                    // does not have BOM
                    _roInput.Position = 0;
                    return;
                }
            }

            // has BOM, and it's skipped now
        }
    }
}
