using System;
using System.Collections.Generic;
using System.Text;

namespace ExtSort.Sorter
{
    public static class TempFilePaths
    {
        public const string SearchPattern = "*.sorttmp*";

        public static string SearchPatternForPhase(int phaseNumber) => $"*.sorttmp{phaseNumber}";

        public static string ExtensionForPhase(int phaseNumber) => $"sorttmp{phaseNumber}";
    }
}
