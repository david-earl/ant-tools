using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

using Illumina.Annotator.Model;
using Illumina.AntTools.Model;

namespace Illumina.AntTools
{
    public class AntProducerConsumer
    {
        private const int ChunkSize = 5000;
        private static readonly int _numWorkerThreads = Environment.ProcessorCount;

        private readonly string _antPath;
        private readonly CancellationToken _cancellationToken;

        private List<Thread> _workerThreads = new List<Thread>(_numWorkerThreads);
        private ConcurrentQueue<HackWrapper<WorkChunk>> _pendingChunks;
        private AntIndex[] _indices = null;

        private int _totalChunkCount = 0;
        private int _chunkFinishedCount = 0;
        private bool _isProducingChunks = true;

        public ConcurrentDictionary<int, AnnotationResult> Annotation { get; private set; }

        public int MemoryAllocationLimitMb { get; set; }

        public Action<double> ProgressCallback { get; set; }

        public bool IsDone
        {
            get { return !_isProducingChunks && _chunkFinishedCount == _totalChunkCount; }
        }


        public AntProducerConsumer(string antPath, CancellationToken cancellationToken = default (CancellationToken))
        {
            _antPath = antPath;

            _cancellationToken = cancellationToken;
        }

        public void Start(List<ChrRange> ranges)
        {
            Init(ranges, (dont, even, care) => true);
        }

        public void Start(Func<int, Variant, bool> variantPredicate)
        {
            Init(null, (i, variant, dontCare) => variantPredicate(i, variant));
        }

        public void Start(Func<Variant, List<ChrRange>, bool> variantPredicate)
        {
            Init(null, (dontCare, variant, ranges) => variantPredicate(variant, ranges));
        }

        private void Init(List<ChrRange> ranges, Func<int, Variant, List<ChrRange>, bool> variantPredicate)
        {          
            _pendingChunks = new ConcurrentQueue<HackWrapper<WorkChunk>>();
            Annotation = new ConcurrentDictionary<int, AnnotationResult>();

            Thread producerThread = new Thread(() => Producer(ranges, _cancellationToken));
            producerThread.Name = "ANT Producer Thread";
            producerThread.Start();

            for (int threadCount = 0; threadCount < _numWorkerThreads; threadCount++)
            {
                Thread workerThread = new Thread(() => AntChunkWorker(_cancellationToken, variantPredicate, ProgressCallback));

                workerThread.Name = String.Format("ANT_chunk_worker_{0}", threadCount + 1);
                workerThread.IsBackground = true;

                _workerThreads.Add(workerThread);

                workerThread.Start();
            }
        }

        private void Producer(List<ChrRange> ranges, CancellationToken cancellationToken)
        {
            string indexFilePath = String.Format("{0}.idx", _antPath);

            _indices = AntIndex.Parse(indexFilePath);

            int rangesIndex = 0;
            ChrRange range = ranges == null ? null : ranges[rangesIndex];

            AntIndex currentIndex = null;
            AntIndex nextIndex = null;

            using (FileStream stream = new FileStream(_antPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan))
            using (BinaryReader reader = new BinaryReader(stream, Encoding.ASCII))
            {
                int indicesIndex = 0;

                // skip the header
                stream.Position = AntHeader.HeaderLength;

                while (_indices.Length == 0 || indicesIndex < _indices.Length)
                { 
                    if (_indices.Length > 0)
                    {
                        currentIndex = _indices[indicesIndex];
                        nextIndex = indicesIndex < _indices.Length - 1 ? _indices[indicesIndex + 1] : null;

                        if (range != null)
                        {
                            if (Chromosome.IsLessThan(currentIndex.Chromosome, range.Chromosome))
                            {
                                if (nextIndex == null || Chromosome.IsLessThan(nextIndex.Chromosome, range.Chromosome))
                                    continue;

                                if (nextIndex.Chromosome == range.Chromosome && nextIndex.ChrPosition < range.StopPosition)
                                    continue;
                            }
                            else if (Chromosome.IsGreaterThan(currentIndex.Chromosome, range.Chromosome))
                            {
                                AntIndex previousIndex = _indices[indicesIndex - 1];

                                if (!Chromosome.IsLessThan(previousIndex.Chromosome, range.Chromosome))
                                    break;

                                // at this point, we know we don't have an index entry matching our chr...
                                // it's still possible that there is annotation that falls between index records, so go back and read the annotation pointed at by the previous index
                                currentIndex = previousIndex;
                            }
                            else if (currentIndex.Chromosome == range.Chromosome)
                            {
                                if (currentIndex.ChrPosition < range.StartPosition)
                                {
                                    if (nextIndex != null && nextIndex.Chromosome.Equals(range.Chromosome) && nextIndex.ChrPosition <= range.StartPosition)
                                        continue;
                                }
                                else if (currentIndex.ChrPosition > range.StopPosition)
                                {
                                    if (ranges != null && ++rangesIndex < ranges.Count())
                                    {
                                        range = ranges[rangesIndex];

                                        indicesIndex--; // so we stay on the same index
                                    }

                                    continue;
                                }
                            }
                        }
                    }

                    if (cancellationToken.IsCancellationRequested)
                    {
                        Console.WriteLine("Aborting the Producer.");
                        
                        return;
                    }

                    List<ChrRange> chunkRanges = _indices == null || ranges == null ? null : ranges.Where(
                             p => p.Chromosome.Equals(currentIndex.Chromosome) && (nextIndex == null || !p.Chromosome.Equals(nextIndex.Chromosome) || (p.StartPosition >= currentIndex.ChrPosition || p.StopPosition < nextIndex.ChrPosition))).ToList();

                    byte[] chunk = ReadChunk(reader);

                    if (chunk == null)
                        break;

                    Interlocked.Increment(ref _totalChunkCount);
                    _pendingChunks.Enqueue(new HackWrapper<WorkChunk>(new WorkChunk(indicesIndex++, chunk, chunkRanges)));
                }

                _isProducingChunks = false;
            }
        }

        private byte[] ReadChunk(BinaryReader reader)
        {
            if (reader.BaseStream.Position == reader.BaseStream.Length)
                return null;

            int chunkLength = reader.ReadInt32();

            byte[] buffer = new byte[chunkLength];

            if (reader.Read(buffer, 0, chunkLength) != chunkLength)
                throw new FileLoadException("An error occurred while parsing the file.");

            return buffer;
        }

        private void AntChunkWorker(CancellationToken cancellationToken, Func<int, Variant, List<ChrRange>, bool> variantPredicate, Action<double> progressCallback = null)
        {
            while (_isProducingChunks || _pendingChunks.Any())
            {
                bool keepTrying = true;
                int retryCount = 0;

                HackWrapper<WorkChunk> chunk = null;

                if (cancellationToken.IsCancellationRequested)
                {
                    Console.WriteLine("Aborting a Consumer.");
                    return;
                }

                while (keepTrying)
                {
                    try
                    {
                        // wait for available chunks from the producer OR throttle based on amount of allocated memory
                        if (!_pendingChunks.Any() || (GC.GetTotalMemory(false) / 1048576) > MemoryAllocationLimitMb)
                        {
                            if (!_isProducingChunks)
                                return;

                            Thread.Sleep(10);

                            continue;
                        }

                        if (cancellationToken.IsCancellationRequested)
                        {
                            Console.WriteLine("Aborting a Consumer.");
                            return;
                        }

                        if (progressCallback != null && Thread.CurrentThread.Name.Equals("ANT_chunk_worker_1"))
                            progressCallback(1.0 - ((double) _pendingChunks.Count() / _indices.Length));

                        if (chunk == null)
                            _pendingChunks.TryDequeue(out chunk);
                        
                        if (chunk == null || chunk.Item == null || chunk.Item.Data == null)
                            continue;

                        int index = chunk.Item.Id; // lambda capture

                        List<AnnotationResult> results = chunk.Item.Data.DeserializeFromBinary((intraChunkIndex, variant) => variantPredicate((index*ChunkSize) + intraChunkIndex, variant, chunk.Item.Ranges)).ToList();

                        chunk.Item = null;
                        chunk = null;

                        int recordCounter = 0;
                        foreach (var foo in results)
                        {
                            int id = (index)*ChunkSize + recordCounter++;

                            foo.Variant.Id = id;

                            Annotation.TryAdd(id, foo);
                        }

                        Interlocked.Increment(ref _chunkFinishedCount);

                        break;
                    }
                    catch (Exception e)
                    {
                        if (retryCount++ > 3)
                            throw;
                    }
                }
            }
        }

    }
}
