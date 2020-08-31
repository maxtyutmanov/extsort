using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace ExtSort.Sorter.IO
{
    public interface IIoManager
    {
        ILineReader CreateReaderForInitialSortPhaseRead(Stream input);

        ILineReader CreateReaderForMergePhaseRead(Stream input);

        ILineWriter CreateWriterForInitialSortPhaseWrite(Stream output, Encoding encoding);

        ILineWriter CreateWriterForMergePhaseWrite(Stream output, Encoding encoding);
    }
}
