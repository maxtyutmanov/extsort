using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ExtSort.Common.Model
{
    public class FastLine : ILine
    {
        private static readonly byte ZeroCodePoint = Convert.ToByte('0');
        private static readonly byte DotCodepoint = Convert.ToByte('.');
        private static readonly byte SpaceCodepoint = Convert.ToByte(' ');
        private static readonly byte CarriageReturnCodepoint = Convert.ToByte('\r');
        private static readonly byte NewLineCodepoint = Convert.ToByte('\n');

        private readonly int _number;
        private readonly byte[] _strBytes;

        private FastLine(int number, byte[] strBytes)
        {
            _number = number;
            _strBytes = strBytes;
        }

        public static FastLine ParseFromStream(Stream stream, List<byte> tmpStrBuffer)
        {
            // only UTF8 is supported

            int b;

            // read number
            var number = 0;
            while ((b = stream.ReadByte()) != '.' && b != -1)
            {
                var isValidDigit = ('0' <= b && b <= '9');
                if (!isValidDigit)
                    return null;

                number *= 10;
                number += b - '0';
            }

            // unexpected end of stream
            if (b == -1)
                return null;

            // skip space
            if (stream.ReadByte() != ' ')
                return null;

            tmpStrBuffer.Clear();
            while ((b = stream.ReadByte()) != '\r' && b != -1)
            {
                tmpStrBuffer.Add((byte)b);
            }

            var strBuffer = tmpStrBuffer.ToArray();
            stream.SkipNewline();
            return new FastLine(number, strBuffer);
        }

        public void WriteToStream(Stream stream, byte[] digitBuffer)
        {
            WriteNumberToStream(stream, digitBuffer);
            stream.WriteByte(DotCodepoint);
            stream.WriteByte(SpaceCodepoint);
            stream.Write(_strBytes, 0, _strBytes.Length);
            stream.WriteByte(CarriageReturnCodepoint);
            stream.WriteByte(NewLineCodepoint);
        }

        public int CompareTo(object otherObj)
        {
            var other = (FastLine)otherObj;

            var strCmp = CompareStrings(_strBytes, other._strBytes);
            if (strCmp != 0)
                return strCmp;

            return _number - other._number;
        }

        private void WriteNumberToStream(Stream stream, byte[] digitBuffer)
        {
            var number = Math.Abs(_number);
            var digit = 0;
            while (number != 0)
            {
                digitBuffer[digit] = (byte)(number % 10);
                digit++;
                number /= 10;
            }

            if (digit == 0)
            {
                stream.WriteByte(Convert.ToByte('0'));
                return;
            }

            // the buffer contains digits in reversed order
            for (var i = digit - 1; i >= 0; i--)
            {
                stream.WriteByte((byte)(digitBuffer[i] + ZeroCodePoint));
            }
        }

        private static int CompareStrings(byte[] str1, byte[] str2)
        {
            var minLength = Math.Min(str1.Length, str2.Length);
            for (int i = 0; i < minLength; i++)
            {
                var b1 = str1[i];
                var b2 = str2[i];

                if (b1 <= 127 && b2 <= 127)
                {
                    // compare as ascii chars (case insensitive)
                    b1 = ToUpper(b1);
                    b2 = ToUpper(b2);
                    var diff = b1 - b2;
                    if (diff != 0)
                    {
                        return diff;
                    }
                }
                else
                {
                    // fallback to slow comparison

                    var decoded1 = Encoding.UTF8.GetString(str1);
                    var decoded2 = Encoding.UTF8.GetString(str2);
                    return string.Compare(decoded1, decoded2, StringComparison.OrdinalIgnoreCase);
                }
            }

            return str1.Length - str2.Length;
        }

        private static byte ToUpper(byte b)
        {
            // 97 is 'a', 122 is 'z'
            if (97 <= b && b <= 122)
                return (byte)(b - 32);
            return b;
        }
    }
}
