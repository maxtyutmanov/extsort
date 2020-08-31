using ExtSort.Common.Model;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace ExtSort.Sorter
{
    public class LineReader : ILineReader
    {
        private readonly GZipStream _gzip;
        private readonly StreamReader _reader;

        public long Position => _reader.BaseStream.Position;

        public bool EndOfStream => _reader.EndOfStream;

        public Encoding InputEncoding => _reader.CurrentEncoding;

        public LineReader(Stream input, int bufferBytes, bool useGzip = false)
        {
            if (useGzip)
                _gzip = new GZipStream(input, CompressionMode.Decompress, true);

            _reader = new StreamReader(_gzip ?? input, bufferSize: bufferBytes, leaveOpen: true);
        }

        public void Dispose()
        {
            _reader.Dispose();
            _gzip?.Dispose();
        }

        public ILine ReadLine()
        {
            // TODO: error handling

            int number;
            string str;

            if (!TryReadNumber(out number))
                return null;

            if (!TrySkipWhitespace())
                return null;

            if (!TryReadString(out str))
                return null;

            return new Line(number, str);
        }

        private bool TryReadNumber(out int number)
        {
            number = 0;
            int c;
            while ((c = ReadChar()) != '.' && c != -1)
            {
                var isValidDigit = ('0' <= c && c <= '9');
                if (!isValidDigit)
                    return false;

                number *= 10;
                number += c - '0';
            }

            // should not be EOF
            if (c == -1)
                return false;

            return true;
        }

        private bool TrySkipWhitespace()
        {
            return ReadChar() == ' ';
        }

        private bool TryReadString(out string str)
        {
            str = _reader.ReadLine();
            return str != null;
        }

        private int ReadChar() => _reader.Read();
    }
}
