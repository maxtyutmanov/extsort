using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ExtSort.Common
{
    public static class StreamExt
    {
        public static bool IsEof(this Stream stream) => stream.Position == stream.Length;

        public static void SkipNewline(this Stream stream)
        {
            if (stream.IsEof())
                return;

            var b = stream.ReadByte();
            if (b == '\r' && !stream.IsEof())
            {
                stream.ReadByte();
            }
        }
    }
}
