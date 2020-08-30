using ExtSort.Generator.Config;
using ExtSort.Generator.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ExtSort.Generator
{
    public interface ILineGenerator
    {
        string Next();
    }

    /// <summary>
    /// Generates random lines in the format "number. string". Not thread safe.
    /// </summary>
    public class LineGenerator : ILineGenerator
    {
        private static readonly IReadOnlyList<char> Alphabet = CreateAlphabet();
        private readonly GeneratorConfig _config;
        private readonly Random _rand;
        private readonly StringBuilder _sb;
        private readonly RingBuffer<string> _prevGeneratedStrings;
        private readonly StringBuilder _prevSb;

        public LineGenerator(GeneratorConfig config = null)
        {
            _config = config ?? new GeneratorConfig();
            _rand = new Random();
            _sb = new StringBuilder();
            _prevSb = new StringBuilder();
            _prevGeneratedStrings = new RingBuffer<string>(1000);
        }

        public string Next()
        {
            // reusing stringbuilder's buffer: we assume that all lines have more or less the same length
            _sb.Clear();
            var number = _rand.Next();
            _sb.Append(number);
            _sb.Append(". ");

            var needDuplicate = _prevGeneratedStrings.Count > 0 && _rand.NextDouble() < _config.DuplicatesProbability;
            if (needDuplicate)
            {
                AppendDuplicate();
            }
            else
            {
                AppendRandString();
            }
            return _sb.ToString();
        }

        private void AppendDuplicate()
        {
            var duplicateIx = _rand.Next(0, _prevGeneratedStrings.Count);
            _sb.Append(_prevGeneratedStrings[duplicateIx]);
        }

        private void AppendRandString()
        {
            _prevSb.Clear();

            var strLength = _rand.Next(_config.MinStringLength, _config.MaxStringLength + 1);

            for (var i = 0; i < strLength; i++)
            {
                var c = Alphabet[_rand.Next(Alphabet.Count)];
                _sb.Append(c);
                _prevSb.Append(c);
            }

            _prevGeneratedStrings.Add(_prevSb.ToString());
        }

        private static List<char> CreateAlphabet()
        {
            // from 'a' to 'z'
            var lowercase = Enumerable
                .Range(Convert.ToInt32('a'), Convert.ToInt32('z') - Convert.ToInt32('a') + 1)
                .Select(Convert.ToChar)
                .ToList();

            // from 'A' to 'Z'
            var uppercase = lowercase.Select(char.ToUpperInvariant);

            return lowercase.Concat(uppercase).ToList();
        }
    }
}
