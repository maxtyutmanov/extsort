using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ExtSort.Common.Model
{
    public class SlowLine : ILine
    {
        private readonly string _str;

        private SlowLine(string str)
        {
            _str = str;
        }

        public static SlowLine ParseFromStream(Stream stream)
        {
            var startPos = stream.Position;
            int b;
            while ((b = stream.ReadByte()) != -1 && b != '\r');
            var endPos = stream.Position;

            if (startPos == endPos)
                return null;

            stream.Position = startPos;
            var strLen = endPos - startPos;
            var strBytes = new byte[strLen];
            stream.Read(strBytes, 0, (int)strLen);
            stream.SkipNewline();
            
            var str = Encoding.UTF8.GetString(strBytes);
            return new SlowLine(str);
        }

        public int CompareTo(object other)
        {
            return ComparisonUtils.CompareFileLines(_str, ((SlowLine)other)._str);
        }

        public void WriteToStream(Stream stream, byte[] digitBuffer)
        {
            throw new NotImplementedException();
        }
    }
}
