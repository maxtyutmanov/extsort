using ExtSort.Common.Model;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace ExtSort.Sorter
{
    public class LineWriter : ILineWriter
    {
        private readonly StreamWriter _writer;
        private readonly GZipStream _gzip;

        public LineWriter(Stream output, Encoding encoding, int bufferBytes, bool useGzip = false)
        {
            if (useGzip)
                _gzip = new GZipStream(output, CompressionMode.Compress, true);

            _writer = new StreamWriter(_gzip ?? output, encoding, bufferBytes, true);
        }

        public void Dispose()
        {
            _writer.Dispose();
            _gzip?.Dispose();
        }

        public void Flush()
        {
            _writer.Flush();
            _gzip?.Flush();
        }

        public void WriteLine(ILine line)
        {
            var lineAs = (Line)line;
            _writer.Write(lineAs.Number);
            _writer.Write(". ");
            _writer.WriteLine(lineAs.Str);
        }
    }
}
