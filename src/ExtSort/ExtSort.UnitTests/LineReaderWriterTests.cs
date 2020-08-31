using ExtSort.Common.Model;
using ExtSort.Sorter;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Xunit;

namespace ExtSort.UnitTests
{
    public class LineReaderWriterTests
    {
        [Fact]
        public void ShouldReadAllLinesFromStream()
        {
            var testLines = new List<string>()
            {
                "415. Apple",
                "30432. Something something something",
                "32. Cherry is the best"
            };

            using var input = GetStreamWithLines(testLines);
            // use small buffer for reader
            using var sut = new LineReader(input, 8);
            var lines = ReadLinesFromReader(sut).ToList();
            lines.Should().AllBeOfType<Line>();
            var lineTexts = lines.Cast<Line>().Select(ConvertLineBackToString).ToList();
            lineTexts.Should().BeEquivalentTo(testLines);
        }

        [Fact]
        public void ReadAndWriteBack_ShouldBeSame()
        {
            var testLines = new List<string>()
            {
                "415. Apple",
                "30432. Something something something",
                "32. Cherry is the best"
            };

            using var input = GetStreamWithLines(testLines);
            using var output = new MemoryStream();
            using var reader = new LineReader(input, 8);
            using var writer = new LineWriter(output, reader.InputEncoding, 8);
            
            foreach (var line in ReadLinesFromReader(reader))
            {
                writer.WriteLine(line);
            }
            writer.Flush();

            input.Position = 0;
            output.Position = 0;
            var inputArr = input.ToArray();
            var outputArr = output.ToArray();

            outputArr.Should().BeEquivalentTo(inputArr);
        }

        private string ConvertLineBackToString(Line l)
        {
            return $"{l.Number}. {l.Str}";
        }

        private IEnumerable<ILine> ReadLinesFromReader(LineReader reader)
        {
            while (!reader.EndOfStream)
            {
                yield return reader.ReadLine();
            }
        }

        private MemoryStream GetStreamWithLines(IEnumerable<string> lines)
        {
            var ms = new MemoryStream();
            var writer = new StreamWriter(ms, Encoding.UTF8);
            lines.ToList().ForEach(l => writer.WriteLine(l));
            writer.Flush();
            ms.Position = 0;
            return ms;
        }
    }
}
