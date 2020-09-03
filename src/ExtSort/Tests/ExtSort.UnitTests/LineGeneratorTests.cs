using ExtSort.Generator;
using FluentAssertions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;

namespace ExtSort.UnitTests
{
    public class LineGeneratorTests
    {
        [Fact]
        public void ShouldCreateOnlyDuplicatesIfDuplicateProbabilityIs1()
        {
            var sut = new LineGenerator(new Generator.Config.GeneratorConfig()
            {
                DuplicatesProbability = 1
            });
            Enumerable.Range(0, 10)
                .Select(_ => sut.Next())
                .Select(GetStringPart)
                .Distinct()
                .Count().Should().Be(1);
        }

        [Fact]
        public void ShouldNotCreateDuplicatesIfDuplicateProbabilityIs0()
        {
            var sut = new LineGenerator(new Generator.Config.GeneratorConfig()
            {
                DuplicatesProbability = 0
            });
            Enumerable.Range(0, 10)
                .Select(_ => sut.Next())
                .Select(GetStringPart)
                .Distinct()
                .Count().Should().Be(10);
        }

        [Fact]
        public void ShouldCreateSomeDuplicatesIfDuplicateProbabilityIsNonZero()
        {
            var sut = new LineGenerator(new Generator.Config.GeneratorConfig()
            {
                DuplicatesProbability = 0.5
            });
            Enumerable.Range(0, 10000)
                .Select(_ => sut.Next())
                .Select(GetStringPart)
                .Distinct()
                .Count().Should().BeLessThan(10000).And.BeGreaterThan(1);
        }

        private static string GetStringPart(string line) => line.Substring(line.IndexOf('.') + 1);
    }
}
