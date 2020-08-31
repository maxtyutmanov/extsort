using System.IO;
using System.Text;
using ExtSort.Common;
using ExtSort.Sorter.Config;

namespace ExtSort.Sorter.IO
{
    public class IoManager : IIoManager
    {
        private readonly FileBuffersConfig _buffers;

        public IoManager(FileBuffersConfig buffers)
        {
            _buffers = buffers;
        }

        public ILineReader CreateReaderForInitialSortPhaseRead(Stream input)
        {
            return new LineReader(input, (int)_buffers.InitialSortInputFileBufferSize, useGzip: false);
        }

        public ILineReader CreateReaderForMergePhaseRead(Stream input)
        {
            return new LineReader(input, (int)_buffers.MergePhaseInputFileBufferSize, useGzip: false);
        }

        public ILineWriter CreateWriterForInitialSortPhaseWrite(Stream output, Encoding encoding)
        {
            return new LineWriter(output, encoding, (int)_buffers.InitialSortOutputFileBufferSize, useGzip: false);
        }

        public ILineWriter CreateWriterForMergePhaseWrite(Stream output, Encoding encoding)
        {
            return new LineWriter(output, encoding, (int)_buffers.MergePhaseOutputFileBufferSize, useGzip: false);
        }
    }
}
