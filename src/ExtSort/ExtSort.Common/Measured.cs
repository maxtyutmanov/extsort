using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace ExtSort.Common
{
    public static class Measured
    {
        private static readonly object _consoleSync = new object();

        public static IDisposable Operation(string operationName)
        {
            var sw = Stopwatch.StartNew();
            return new MeasuredOperation(operationName, sw);
        }

        private class MeasuredOperation : IDisposable
        {
            private readonly string _operationName;
            private readonly Stopwatch _sw;

            public MeasuredOperation(string operationName, Stopwatch sw)
            {
                _operationName = operationName;
                _sw = sw;
            }

            public void Dispose()
            {
                lock (_consoleSync)
                {
                    Console.WriteLine("Operation '{0}' took {1}", _operationName, _sw.Elapsed);
                }
            }
        }
    }
}
