using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using Illumina.AntTools.Model;

namespace Illumina.AntTools
{
    public class AntReader
    {
        // this smells bad, but it avoids an IAS dep
        private static readonly Dictionary<int, Tuple<string, TranscriptSource>> _datasetInfoByAnnotationCollectionId = new Dictionary<int, Tuple<string, TranscriptSource>>()
        {
            {3, new Tuple<string, TranscriptSource>("72.4", TranscriptSource.Ensembl) },
            {4, new Tuple<string, TranscriptSource>("72.4", TranscriptSource.RefSeq) },
            {5, new Tuple<string, TranscriptSource>("72.5", TranscriptSource.Ensembl) },
            {6, new Tuple<string, TranscriptSource>("72.5", TranscriptSource.RefSeq) },
            {7, new Tuple<string, TranscriptSource>("75.2", TranscriptSource.Ensembl) },
            {8, new Tuple<string, TranscriptSource>("75.2", TranscriptSource.RefSeq) },
        };

        private List<AnnotationResult>[] _tempResults;
        private BlockingCollection<AnnotationResult> _annotation;

        private AntIndex[] _indices = null;
        private ConcurrentQueue<Tuple<int, byte[], ChrRange>> _chunks;
        private Stats _antStats;
        private object _statsLock = new object();
        private bool _isCancelled = false;
        private bool _isProducingChunks = true;
        private int _chunkFinishedCount = 0;

        public readonly byte _antFormatNumber = 3;

        private static readonly int _numWorkerThreads = Environment.ProcessorCount;
        private List<Thread> _workerThreads = new List<Thread>(_numWorkerThreads); 

        private readonly string _antPath;


        public string AntVersion
        {
            get { return String.Format("ANT{0}", _antFormatNumber); }
        }

        public Action<double> ProgressCallback { get; set; }


        public AntReader(string filepath)
        {
            _antPath = filepath;
        }


        public IEnumerable<AnnotationResult> Load(out int annotationCollectionId, ChrRange range = null)
        {
            annotationCollectionId = Init(range);

            return _annotation.GetConsumingEnumerable();
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
            int annotationCollectionId = Init(null, false, true);

            Tuple<string, TranscriptSource> dataInfo = _datasetInfoByAnnotationCollectionId.ContainsKey(annotationCollectionId) ? _datasetInfoByAnnotationCollectionId[annotationCollectionId] : new Tuple<string, TranscriptSource>("unknown", TranscriptSource.RefSeq);

            _antStats = new Stats() { DatasetVersion = dataInfo.Item1, TranscriptSource = dataInfo.Item2, Ranges = new List<ChrRange>() };

            Console.Write("Generating ANT stats...");

            int cursorRow = Console.CursorTop;
            int cursorColumn = Console.CursorLeft;

            ProgressCallback = (progress) =>
            {
                Console.SetCursorPosition(cursorColumn, cursorRow);
                Console.Write("{0}%", (progress*100).ToString("0.00"));
            }; 

            foreach (var dontCare in _annotation.GetConsumingEnumerable())
            {
                // wait for processing to complete
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


        private int Init(ChrRange range = null, bool isValidating = false, bool isStats = false)
        {
            _chunks = new ConcurrentQueue<Tuple<int, byte[], ChrRange>>();
            _annotation = new BlockingCollection<AnnotationResult>();

            int annotationCollectionId = ParseAntHeader(_antPath);

            Thread producerThread = new Thread(() => Producer(range));
            producerThread.Name = "ANT Producer Thread";
            producerThread.Start();

            for (int threadCount = 0; threadCount < _numWorkerThreads; threadCount++)
            {
                Thread workerThread = new Thread(() => AntChunkWorker(isStats, isValidating));

                workerThread.Name = String.Format("ANT_chunk_worker_{0}", threadCount + 1);
                workerThread.IsBackground = true;

                _workerThreads.Add(workerThread);

                workerThread.Start();
            }

            Thread yieldThread = new Thread(YieldWorker);
            yieldThread.Name = "ANT Yield Thread";
            yieldThread.Start();

            return annotationCollectionId;
        }

        private void Producer(ChrRange range)
        {
            _isProducingChunks = true;

            string indexFilePath = String.Format("{0}.idx", _antPath);

            if (!File.Exists(indexFilePath))
                throw new Exception("Can't find index file.");

            _indices = ParseAntIndex(indexFilePath);

            _tempResults = new List<AnnotationResult>[_indices.Count()];

            // if the user doesn't specify any indexing, default to everything
            if (range == null)
            {

            }

            using (FileStream stream = File.Open(_antPath, FileMode.Open))
            using (BinaryReader reader = new BinaryReader(stream, Encoding.ASCII))
            {
                for (int indicesIndex = 0; indicesIndex < _indices.Length; indicesIndex++)
                {
                    AntIndex currentIndex = _indices[indicesIndex];

                    if (range != null)
                    {
                        if (Chromosome.IsLessThan(currentIndex.Chromosome, range.Chromosome))
                        {
                            if (indicesIndex == _indices.Length - 1)
                                continue;

                            AntIndex nextIndex = _indices[indicesIndex + 1];

                            if (Chromosome.IsLessThan(nextIndex.Chromosome, range.Chromosome))
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
                                AntIndex nextIndex = _indices[indicesIndex + 1];

                                if (nextIndex.ChrPosition <= range.StartPosition)
                                    continue;
                            }
                            else if (currentIndex.ChrPosition > range.StopPosition)
                            {
                                // TODO: at this point we should be able to safely break the loop

                                continue;
                            }
                        }
                    }

                    reader.BaseStream.Position = currentIndex.FilePosition;

                    _chunks.Enqueue(new Tuple<int, byte[], ChrRange>(indicesIndex, ReadChunk(reader), range));
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

            byte[] copy = new byte[chunkLength];
            Buffer.BlockCopy(buffer, 0, copy, 0, chunkLength);

            return copy;
        }

        private void AntChunkWorker(bool isStatsLoad = false, bool isValidating = false)
        {
            int chunkIndex = -1;
            byte[] buffer = null;
            ChrRange range = null;

            try
            {
                while (_isProducingChunks || _chunks.Any())
                {
                    if (!_chunks.Any())
                    {
                        Thread.Sleep(10);

                        continue;
                    }

                    if (ProgressCallback != null && Thread.CurrentThread.Name.Equals("ANT_chunk_worker_1"))
                        ProgressCallback(1.0 - ((double) _chunks.Count() / _indices.Length));

                    Tuple<int, byte[], ChrRange> chunk;

                    _chunks.TryDequeue(out chunk);
                    
                    if (chunk == null || chunk.Item2 == null)
                        continue;

                    chunkIndex = chunk.Item1;
                    buffer = chunk.Item2;
                    range = chunk.Item3;

                    List<AnnotationResult> results = buffer.DeserializeFromBinary(variant => VariantPredicate(variant, range, isStatsLoad, isValidating)).ToList();

                    if (!isStatsLoad)
                        _tempResults[chunkIndex] = results;

                    Interlocked.Increment(ref _chunkFinishedCount);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Thread exception: {0}", e.Message);

                // bail on this thread and let one of the others pick it up

                if (buffer != null && _chunks != null && chunkIndex > 0)
                    _chunks.Enqueue(new Tuple<int, byte[], ChrRange>(chunkIndex, buffer, range));
            }
        }

        private void YieldWorker()
        {
            int chunkIndex = 0;

            // wait for the Producer thread to parse the index file
            while (_indices == null)
            {
                Thread.Sleep(10);
            }

            while (chunkIndex < _indices.Count())
            {
                if (_tempResults[chunkIndex] == null)
                {
                    // if we expect that a chunk might could be added, wait for it; otherwise increment the index and continue
                    if (!_isProducingChunks && !_workerThreads.Any(p => p.IsAlive))
                        chunkIndex++;
                    else
                        Thread.Sleep(100);

                    continue;
                }

                List<AnnotationResult> chunkResults = _tempResults[chunkIndex++];

                int recordCounter = 1;
                foreach (AnnotationResult result in chunkResults)
                {
                    result.Variant.Id = (chunkIndex - 1)*chunkResults.Count() + recordCounter++; 
                    _annotation.Add(result);
                }
            }

            _annotation.CompleteAdding();
        }

        private bool VariantPredicate(Variant variant, ChrRange range, bool isStatsLoad, bool isValidating)
        {
            if (isValidating)
                return false;

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

            if (range == null)
                return true;

            return variant.Chromosome == range.Chromosome && range.StartPosition <= variant.Position && variant.Position <= range.StopPosition;
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

    }
}
