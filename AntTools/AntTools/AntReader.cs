using System;
using System.CodeDom;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

using Illumina.Annotator.Model;
using Illumina.AntTools.Model;

namespace Illumina.AntTools
{
    public class AntReader
    {
        private const int ChunkSize = 5000;

        // this smells bad, but it avoids an IAS dep.  It needs to be manually curated to match the latest IAS AnnotationCollection state
        internal static readonly Dictionary<int, Tuple<string, TranscriptSource>> _datasetInfoByAnnotationCollectionId = new Dictionary<int, Tuple<string, TranscriptSource>>()
        {
            {3, new Tuple<string, TranscriptSource>("72.4", TranscriptSource.Ensembl) },
            {4, new Tuple<string, TranscriptSource>("72.4", TranscriptSource.RefSeq) },
            {5, new Tuple<string, TranscriptSource>("72.5", TranscriptSource.Ensembl) },
            {6, new Tuple<string, TranscriptSource>("72.5", TranscriptSource.RefSeq) },
            {7, new Tuple<string, TranscriptSource>("75.2", TranscriptSource.Ensembl) },
            {8, new Tuple<string, TranscriptSource>("75.2", TranscriptSource.RefSeq) },
        };

        private ConcurrentQueue<HackWrapper<Chunk>> _chunks;
        private List<AnnotationResult>[] _tempResults;
        private ConcurrentDictionary<int, AnnotationResult> _annotation;

        private AntIndex[] _indices = null;
        private Stats _antStats;

        private object _statsLock = new object();
        private bool _isCancelled = false;
        private bool _isProducingChunks = true;
        private int _chunkFinishedCount = 0;

        public readonly byte _antFormatNumber = 3;

        private static readonly int _numWorkerThreads = Environment.ProcessorCount;
        private List<Thread> _workerThreads = new List<Thread>(_numWorkerThreads);

        private Func<int, Variant, bool> _userVariantPredicate;

        private CancellationToken _cancellationToken;

        private readonly string _antPath;
        private readonly int _annotationCollectionId;


        public string AntVersion
        {
            get { return String.Format("ANT{0}", _antFormatNumber); }
        }

        public int MemoryAllocationLimitMb { get; set; }

        public Action<double> ProgressCallback { get; set; }


        public AntReader(string filepath, out int annotationCollectionId, CancellationToken cancellationToken = default (CancellationToken))
        {
            _antPath = filepath;

            _annotationCollectionId = ParseAntHeader(_antPath);

            annotationCollectionId = _annotationCollectionId;

            MemoryAllocationLimitMb = 2048;

            _cancellationToken = cancellationToken; 
        }


        public IEnumerable<AnnotationResult> Load(List<ChrRange> ranges = null)
        {
            Init(ranges);

            int recordIndex = 0;

            while (_indices == null)
            {
                Thread.Sleep(10);
            }

            while (_chunkFinishedCount < _indices.Count() || _annotation.Any())
            {
                if (_cancellationToken.IsCancellationRequested)
                    yield break;

                if (!_annotation.ContainsKey(recordIndex))
                {
                    Thread.Sleep(10);

                    continue;
                }

                yield return _annotation[recordIndex];

                AnnotationResult dontCare;
                _annotation.TryRemove(recordIndex++, out dontCare);
            }
        }

        public IEnumerable<AnnotationResult> Load(Func<int, Variant, bool> variantPredicate)
        {
            _userVariantPredicate = variantPredicate;

            return Load();
        }

        public bool Validate()
        {
            Console.Write("Validating ANT file...");

            try
            {
                Init(null, true, false);

                Console.WriteLine("valid.");
            }
            catch (Exception e)
            {
                Console.WriteLine("invalid: {0}.", e.Message);

                return false;
            }

            return true;
        }

        public void PrintAntStats()
        {
            Init(null, false, true);

            Tuple<string, TranscriptSource> dataInfo = _datasetInfoByAnnotationCollectionId.ContainsKey(_annotationCollectionId) ? _datasetInfoByAnnotationCollectionId[_annotationCollectionId] : new Tuple<string, TranscriptSource>("unknown", TranscriptSource.RefSeq);

            _antStats = new Stats() { DatasetVersion = dataInfo.Item1, TranscriptSource = dataInfo.Item2, Ranges = new List<ChrRange>() };

            Console.Write("Generating ANT stats...");

            int cursorRow = Console.CursorTop;
            int cursorColumn = Console.CursorLeft;

            ProgressCallback = (progress) =>
            {
                Console.SetCursorPosition(cursorColumn, cursorRow);
                Console.Write("{0}%", (progress*100).ToString("0.00"));
            }; 

            int recordIndex = 0;
            while (_chunkFinishedCount < _indices.Count() || _annotation.Any())
            {
                if (!_annotation.ContainsKey(recordIndex))
                {
                    Thread.Sleep(10);

                    continue;
                }

                AnnotationResult dontCare;
                _annotation.TryRemove(recordIndex++, out dontCare);
            }

            Console.WriteLine("\n\rANT stats:");
            Console.WriteLine("\tDataset Version: {0}", _antStats.DatasetVersion);
            Console.WriteLine("\tTranscriptSource: {0}", _antStats.TranscriptSource);
            Console.WriteLine("\t# Annotated Variants: {0}", _antStats.VariantCount);
            Console.WriteLine("\tChr Ranges:");

            foreach(ChrRange range in _antStats.Ranges)
            {
                Console.WriteLine("\t\t{0}:{1}-{2}", range.Chromosome, range.StartPosition, range.StopPosition);
            }
        }

        /// <summary>
        /// Parses an Illumina binary annotation index (.ant.idx).
        /// </summary>
        /// <param name="filepath">A fully qualified path to the index file.</param>
        /// <returns>An array of AntIndex objects.</returns>
        public AntIndex[] ParseAntIndex(string filepath)
        {
            if (!File.Exists(filepath))
                return new AntIndex[0];

            List<AntIndex> indices = new List<AntIndex>();

            using (StreamReader streamReader = new StreamReader(filepath))
            {
                string line;

                while ((line = streamReader.ReadLine()) != null)
                {
                    string[] splits = line.Split('\t');

                    if (splits.Length != 3)
                        throw new FileLoadException("Improperly formatted index file.");

                    indices.Add(new AntIndex(splits[0], splits[1], splits[2]));
                }
            }

            return indices.ToArray();
        }


        private void Init(List<ChrRange> ranges = null, bool isValidating = false, bool isStats = false)
        {          
            _chunks = new ConcurrentQueue<HackWrapper<Chunk>>();
            _annotation = new ConcurrentDictionary<int, AnnotationResult>();

            Thread producerThread = new Thread(() => Producer(ranges, _cancellationToken));
            producerThread.Name = "ANT Producer Thread";
            producerThread.Start();

            for (int threadCount = 0; threadCount < _numWorkerThreads; threadCount++)
            {
                Thread workerThread = new Thread(() => AntChunkWorker(_cancellationToken, isStats, isValidating));

                workerThread.Name = String.Format("ANT_chunk_worker_{0}", threadCount + 1);
                workerThread.IsBackground = true;

                _workerThreads.Add(workerThread);

                workerThread.Start();
            }
        }

        private void Producer(List<ChrRange> ranges, CancellationToken cancellationToken)
        {
            _isProducingChunks = true;

            string indexFilePath = String.Format("{0}.idx", _antPath);

            if (!File.Exists(indexFilePath))
                throw new Exception("Can't find index file.");

            _indices = ParseAntIndex(indexFilePath);

            _tempResults = new List<AnnotationResult>[_indices.Count()];

            int rangesIndex = 0;
            ChrRange range = ranges == null ? null : ranges[rangesIndex];

            AntIndex currentIndex;
            AntIndex nextIndex;

            using (FileStream stream = new FileStream(_antPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan))
            using (BinaryReader reader = new BinaryReader(stream, Encoding.ASCII))
            {
                for (int indicesIndex = 0; indicesIndex < _indices.Length; indicesIndex++)
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

                    if (cancellationToken.IsCancellationRequested)
                        return;

                    reader.BaseStream.Position = currentIndex.FilePosition;

                    List<ChrRange> chunkRanges = ranges == null ? null : ranges.Where(
                             p => p.Chromosome.Equals(currentIndex.Chromosome) && (nextIndex == null || !p.Chromosome.Equals(nextIndex.Chromosome) || (p.StartPosition >= currentIndex.ChrPosition || p.StopPosition < nextIndex.ChrPosition))).ToList();

                    _chunks.Enqueue(new HackWrapper<Chunk>(new Chunk(indicesIndex, ReadChunk(reader), chunkRanges)));
                }

                _isProducingChunks = false;
            }
        }

        private byte[] ReadChunk(BinaryReader reader)
        {
            int chunkLength = reader.ReadInt32();

            byte[] buffer = new byte[chunkLength];

            if (reader.Read(buffer, 0, chunkLength) != chunkLength)
                throw new FileLoadException("An error occurred while parsing the file.");

            return buffer;
        }

        private void AntChunkWorker(CancellationToken cancellationToken, bool isStatsLoad = false, bool isValidating = false)
        {
            while (_isProducingChunks || _chunks.Any())
            {
                bool keepTrying = true;
                int retryCount = 0;

                HackWrapper<Chunk> chunk = null;

                if (cancellationToken.IsCancellationRequested)
                    return;

                while (keepTrying)
                {
                    try
                    {
                        // wait for available chunks from the producer OR throttle based on amount of allocated memory
                        if (!_chunks.Any() || (GC.GetTotalMemory(false) / 1048576) > MemoryAllocationLimitMb)
                        {
                            Thread.Sleep(10);

                            continue;
                        }

                        if (ProgressCallback != null && Thread.CurrentThread.Name.Equals("ANT_chunk_worker_1"))
                            ProgressCallback(1.0 - ((double) _chunks.Count() / _indices.Length));

                        if (chunk == null)
                            _chunks.TryDequeue(out chunk);
                        
                        if (chunk == null || chunk.Item == null || chunk.Item.Data == null)
                            continue;

                        int index = chunk.Item.Id; // lambda capture

                        List<AnnotationResult> results = chunk.Item.Data.DeserializeFromBinary((intraChunkIndex, variant) => VariantPredicate((index*ChunkSize) + intraChunkIndex, variant, chunk.Item.Ranges, isStatsLoad, isValidating)).ToList();

                        chunk.Item = null;
                        chunk = null;

                        int recordCounter = 0;
                        foreach (var foo in results)
                        {
                            int id = (index)*ChunkSize + recordCounter++;

                            foo.Variant.Id = id;

                            _annotation.TryAdd(id, foo);
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

        private bool VariantPredicate(int variantIndex, Variant variant, List<ChrRange> ranges, bool isStatsLoad, bool isValidating)
        {
            if (isValidating)
                return false;

            if (_userVariantPredicate != null)
                return _userVariantPredicate(variantIndex, variant);

            if (isStatsLoad)
            {
                lock (_statsLock)
                {
                    _antStats.VariantCount++;

                    ChrRange chrRange = _antStats.Ranges.SingleOrDefault(p => p.Chromosome == variant.Chromosome);

                    if (chrRange == null)
                    {
                        chrRange = new ChrRange() { Chromosome = variant.Chromosome, StartPosition = variant.Position };
                        _antStats.Ranges.Add(chrRange);

                        // guess the transcript source, if necessary
                        if (_antStats.DatasetVersion == "unknown")
                            _antStats.TranscriptSource = variant.Chromosome.StartsWith("chr") ? TranscriptSource.RefSeq : TranscriptSource.Ensembl;
                    }

                    // assume .ant is ordered
                    chrRange.StopPosition = variant.Position;
                }

                return false;
            }

            if (ranges == null || !ranges.Any())
                return true;

            return ranges.Any(p => variant.Chromosome == p.Chromosome && p.StartPosition <= variant.Position && variant.Position <= p.StopPosition);
        }

        private int ParseAntHeader(string filepath)
        {
            int annotationCollectionId;

            using (FileStream stream = File.Open(filepath, FileMode.Open))
            using (BinaryReader reader = new BinaryReader(stream, Encoding.ASCII))
            {
                // read in the header
                byte[] header = new byte[40];
                if (reader.Read(header, 0, 40) != 40)
                    throw new FileLoadException("Invalid .ant header.");

                string version = Encoding.ASCII.GetString(header.Take(3).ToArray()) + header[3];

                if (!(version).Equals(AntVersion))
                    throw new Exception("Unknown ANT version specified.");

                annotationCollectionId = BitConverter.ToInt32(header.Skip(4).Take(4).ToArray(), 0);

                byte[] md5One = header.Skip(8).Take(16).ToArray();
                byte[] md5Two = header.Skip(24).Take(16).ToArray();
            }

            return annotationCollectionId;
        }

        // NOTE: dumb wrapper to work around an issue in which .NET does not release memory allocated for objects removed from a BlockingCollection
        // see: http://stackoverflow.com/questions/12824519/the-net-concurrent-blockingcollection-has-a-memory-leak
        internal class HackWrapper<T>
            where T : class
        {
            public T Item { get; set; }

            public HackWrapper(T item) { Item = item; }
        }

        internal class Chunk
        {
            public int Id { get; set; }

            public byte[] Data { get; set; }

            public List<ChrRange> Ranges { get; set; }

            public Chunk(int id, byte[] data, List<ChrRange> ranges)
            {
                Id = id;
                Data = data;
                Ranges = ranges;
            }
        }
    }
}
