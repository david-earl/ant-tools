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

        private BlockingCollection<AnnotationResult> _annotation;

        private ConcurrentQueue<Tuple<int, byte[], ChrRange>> _chunks;
        private bool _isCancelled = false;
        private bool _isReadingChunks = true;
        private int _chunkFinishedCount = 0;

        public readonly byte _antFormatNumber = 3;

        private readonly int _numWorkerThreads = 1;

        private readonly string _antPath;

        public string AntVersion
        {
            get { return String.Format("ANT{0}", _antFormatNumber); }
        }

        private Stats _antStats;

        public AntReader(string filepath)
        {
            _antPath = filepath;
        }


        public IEnumerable<AnnotationResult> Load(out int annotationCollectionId, ChrRange range = null)
        {
            _chunks = new ConcurrentQueue<Tuple<int, byte[], ChrRange>>();
            _annotation = new BlockingCollection<AnnotationResult>();

            annotationCollectionId = ParseAntHeader(_antPath);

            Thread producerThread = new Thread(() => Producer(range));
            producerThread.Name = "ANT Producer Thread";
            producerThread.Start();

            for (int threadCount = 0; threadCount < _numWorkerThreads; threadCount++)
            {
                Thread thread = new Thread(() => AntChunkWorker(false));

                thread.Name = String.Format("ANT_chunk_worker_{0}", threadCount + 1);
                thread.IsBackground = true;

                thread.Start();
            }

            return _annotation.GetConsumingEnumerable();
        }

        public void PrintAntStats()
        {
            _chunks = new ConcurrentQueue<Tuple<int, byte[], ChrRange>>();
            _annotation = new BlockingCollection<AnnotationResult>();

            int annotationCollectionId = ParseAntHeader(_antPath);

            Tuple<string, TranscriptSource> dataInfo = _datasetInfoByAnnotationCollectionId.ContainsKey(annotationCollectionId) ? _datasetInfoByAnnotationCollectionId[annotationCollectionId] : new Tuple<string, TranscriptSource>("unknown", TranscriptSource.RefSeq);

            _antStats = new Stats() { DatasetVersion = dataInfo.Item1, TranscriptSource = dataInfo.Item2, Ranges = new List<ChrRange>() };

            Thread producerThread = new Thread(() => Producer(null));
            producerThread.Name = "ANT Producer Thread";
            producerThread.Start();

            Thread thread = new Thread(() => AntChunkWorker(true));

            thread.Name = "ANT_chunk_worker";
            thread.IsBackground = true;

            thread.Start();

            Console.WriteLine("Generating ANT stats...\n\r");

            foreach (var dontCare in _annotation.GetConsumingEnumerable())
            {
                // wait for processing to complete
            }

            Console.WriteLine("ANT stats:");
            Console.WriteLine("\tDataset Version: {0}", _antStats.DatasetVersion);
            Console.WriteLine("\tTranscriptSource: {0}", _antStats.TranscriptSource);
            Console.WriteLine("\t# Annotated Variants: {0}", _antStats.VariantCount);
            Console.WriteLine("\tChr Ranges:");

            foreach(ChrRange range in _antStats.Ranges)
            {
                Console.WriteLine("\t\t{0}:{1}-{2}", range.Chromosome, range.StartPosition, range.StopPosition);
            }
        }


        private void Producer(ChrRange range)
        {
            _isReadingChunks = true;

            string indexFilePath = String.Format("{0}.idx", _antPath);

            if (!File.Exists(indexFilePath))
                throw new Exception("Can't find index file.");

            AntIndex[] indices = ParseAntIndex(indexFilePath);

            // if the user doesn't specify any indexing, default to everything
            if (range == null)
            {

            }

            using (FileStream stream = File.Open(_antPath, FileMode.Open))
            using (BinaryReader reader = new BinaryReader(stream, Encoding.ASCII))
            {
                for (int indicesIndex = 0; indicesIndex < indices.Length; indicesIndex++)
                {
                    AntIndex currentIndex = indices[indicesIndex];

                    if (range != null)
                    {
                        if (Chromosome.IsLessThan(currentIndex.Chromosome, range.Chromosome))
                        {
                            if (indicesIndex == indices.Length - 1)
                                continue;

                            AntIndex nextIndex = indices[indicesIndex + 1];

                            if (Chromosome.IsLessThan(nextIndex.Chromosome, range.Chromosome))
                                continue;
                        }
                        else if (Chromosome.IsGreaterThan(currentIndex.Chromosome, range.Chromosome))
                        {
                            AntIndex previousIndex = indices[indicesIndex - 1];

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
                                AntIndex nextIndex = indices[indicesIndex + 1];

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

                _isReadingChunks = false;
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

        private void AntChunkWorker(bool isStatsLoad = false)
        {
            int chunkIndex = -1;
            byte[] buffer = null;
            ChrRange range = null;

            try
            {
                while (_isReadingChunks || _chunks.Any())
                {
                    if (!_chunks.Any())
                    {
                        Thread.Sleep(100);

                        continue;
                    }

                    Tuple<int, byte[], ChrRange> chunk;

                    _chunks.TryDequeue(out chunk);

                    if (chunk.Item2 == null)
                        continue;

                    chunkIndex = chunk.Item1;
                    buffer = chunk.Item2;
                    range = chunk.Item3;

                    List<AnnotationResult> results = buffer.DeserializeFromBinary(variant => VariantPredicate(variant, range, isStatsLoad)).ToList();

                    if (!isStatsLoad)
                    {
                        foreach (AnnotationResult record in results)
                        {
                            _annotation.Add(record);
                        }
                    }

                    Interlocked.Increment(ref _chunkFinishedCount);
                }

                _annotation.CompleteAdding();
            }
            catch (Exception e)
            {
                Console.WriteLine("Thread exception: {0}", e.Message);

                // bail on this thread and let one of the others pick it up

                if (buffer != null && _chunks != null && chunkIndex > 0)
                    _chunks.Enqueue(new Tuple<int, byte[], ChrRange>(chunkIndex, buffer, range));
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

        private bool VariantPredicate(Variant variant, ChrRange range, bool isStatsLoad)
        {
            if (isStatsLoad)
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
