﻿using System;
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

        private BlockingCollection<AnnotationResult> _annotation;

        private ConcurrentQueue<Tuple<int, byte[], ChrRange>> _chunks;
        private bool _isCancelled = false;
        private bool _isReadingChunks = true;
        private int _chunkFinishedCount = 0;

        //private List<string> Chromosomes
        //{
        //    get { return Chromosome.ChrsByTranscriptSource[TranscriptSource]; }
        //}

        public readonly byte _antFormatNumber = 3;

        private readonly int _numWorkerThreads = 1;

        public string AntVersion
        {
            get { return String.Format("ANT{0}", _antFormatNumber); }
        }


        public AntReader()
        {

        }

        public IEnumerable<AnnotationResult> Load(string filepath, out int annotationCollectionId, ChrRange range = null)
        {
            _chunks = new ConcurrentQueue<Tuple<int, byte[], ChrRange>>();
            _annotation = new BlockingCollection<AnnotationResult>();

            annotationCollectionId = ParseAntHeader(filepath);

            int collectionIdCapture = annotationCollectionId;

            Thread producerThread = new Thread(() => Producer(filepath, range));
            producerThread.Name = "ANT Producer Thread";
            producerThread.Start();

            for (int threadCount = 0; threadCount < _numWorkerThreads; threadCount++)
            {
                Thread thread = new Thread(AntChunkWorker);

                thread.Name = String.Format("ANT_chunk_worker_{0}", threadCount + 1);
                thread.IsBackground = true;

                thread.Start();
            }

            return _annotation.GetConsumingEnumerable();
        }


        private void Producer(string filePath, ChrRange range)
        {
            _isReadingChunks = true;

            string indexFilePath = String.Format("{0}.idx", filePath);

            if (!File.Exists(indexFilePath))
                throw new Exception("Can't find index file.");

            AntIndex[] indices = ParseAntIndex(indexFilePath);

            // if the user doesn't specify any indexing, default to everything
            if (range == null)
            {

            }

            //int indicesIndex = 0;
            //int currentChrIndex = 0;

            using (FileStream stream = File.Open(filePath, FileMode.Open))
            using (BinaryReader reader = new BinaryReader(stream, Encoding.ASCII))
            {
                for (int indicesIndex = 0; indicesIndex < indices.Length; indicesIndex++)
                {
                    AntIndex currentIndex = indices[indicesIndex];

                    if (range != null)
                    {
                        if (Chromosome.IsLessThan(currentIndex.Chromosome, range.Chromosome))
                        {
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

                            if (currentIndex.ChrPosition > range.StartPosition)
                            {

                                //// it's possible that the previous index's chromosome didn't match, but the chromosome we're looking for *does* exist towards the end of the previous index's chunk
                                
                                //AntIndex previousIndex = indices[indicesIndex - 1];

                                //if (Chromosome.IsLessThan(previousIndex.Chromosome, range.Chromosome))
                                //{
                                //    reader.BaseStream.Position = previousIndex.FilePosition;

                                //    _chunks.Enqueue(new Tuple<int, byte[], ChrRange>(indicesIndex - 1, ReadChunk(reader), range));
                                //}
                            }
                        }
                    }

                    reader.BaseStream.Position = currentIndex.FilePosition;

                    _chunks.Enqueue(new Tuple<int, byte[], ChrRange>(indicesIndex, ReadChunk(reader), range));
                }

                _isReadingChunks = false;

                    //currentChrIndex = Chromosomes.IndexOf(range.Chromosome);

                    //bool isSeeking = true;
                    //// seek until we find the first AntIndex matching our chr
                    //while (isSeeking)
                    //{
                    //    if (indices[indicesIndex].Chromosome == range.Chromosome)

                    //    string nextAntIndexChr = indices[indicesIndex + 1].Chromosome;

                    //    if (nextAntIndexChr == range.Chromosome) // the next AntIndex matches our current chr...go with it.
                    //    {
                    //        indicesIndex++;

                    //        break;
                    //    }
                    //    else if (Chromosomes.IndexOf(nextAntIndexChr) > currentChrIndex) // the next AntIndex has gone past our current chr...look inside the current AntIndex chunk
                    //        break;
                    //}

            }
        }

        //private List<AnnotationResult>[] LoadFromIndexedBinaryFile(string filepath, int[] indices, out int annotationCollectionId, Func<double, bool> progressCallback, bool[] indexMask = null)
        //{

        //    if (!File.Exists(filepath))
        //        throw new ArgumentException(String.Format("The specified file ({0}) does not exist.", filepath));

        //    long[] indexedFilePositions = ParseAntIndex(filepath + ".idx").Select(p => p.FilePosition).ToArray();

        //    // if no indices given, default to all of them
        //    if (indexedFilePositions.Length > 0 && indices == null)
        //    {
        //        indices = new int[indexedFilePositions.Length];

        //        for (int i = 0; i < indexedFilePositions.Length; i++)
        //        {
        //            indices[i] = i;
        //        }
        //    }

        //    ConcurrentDictionary<int, List<AnnotationResult>> annotation = new ConcurrentDictionary<int, List<AnnotationResult>>();

        //    try
        //    {
        //        List<Thread> workerThreads;
        //        _chunks = new ConcurrentQueue<Tuple<int, byte[]>>();
        //        _isReadingChunks = true;
        //        _isCancelled = false;

        //        using (FileStream stream = File.Open(filepath, FileMode.Open))
        //        using (BinaryReader reader = new BinaryReader(stream, Encoding.ASCII))
        //        {

        //            annotationCollectionId = ParseAntHeader(header, filepath);

        //            workerThreads = new List<Thread>();
        //            for (int threadCount = 0; threadCount < Environment.ProcessorCount + 1; threadCount++)
        //            {
        //                Thread thread = new Thread(() => AntChunkWorker(annotation, indexMask);

        //                workerThreads.Add(thread);

        //                thread.Name = String.Format("ANT_chunk_worker_{0}", threadCount + 1);
        //                thread.IsBackground = true;

        //                thread.Start();
        //            }

        //            _chunkFinishedCount = 0;

        //            if (indexedFilePositions.Length == 0 || indices == null)
        //                ParseAntChunksWithoutIndex(reader);
        //            else
        //                ParseAntChunksWithIndex(reader, indexedFilePositions, indices);


        //            _isReadingChunks = false;

        //            foreach (Thread workerThread in workerThreads)
        //            {
        //                workerThread.Join();
        //            }
        //        }

        //        if (progressCallback != null)
        //            progressCallback(1.0);
        //    }
        //    catch (Exception)
        //    {
        //        throw;
        //    }

        //    _chunks = null;

        //    if (_isCancelled)
        //    {
        //        annotation = null;
        //    }

        //    return annotation.OrderBy(p => p.Key).Select(p => p.Value).ToArray();
        //}

        //private void ParseAntChunksWithoutIndex(BinaryReader reader)
        //{
        //    int chunkCounter = 0;

        //    while (reader.BaseStream.Position < reader.BaseStream.Length)
        //    {
        //        byte[] data = ReadChunk(reader);

        //        _chunks.Enqueue(new Tuple<int, byte[]>(chunkCounter++, data));
        //    }
        //}

        //private void ParseAntChunksWithIndex(BinaryReader reader, long[] indexedFilePositions, int[] indices)
        //{
        //    for (int i = 0; i < indices.Length; i++)
        //    {
        //        reader.BaseStream.Position = indexedFilePositions[indices[i]];

        //        byte[] data = ReadChunk(reader);

        //        _chunks.Enqueue(new Tuple<int, byte[]>(i, data));
        //    }
        //}

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

        private void AntChunkWorker()
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

                    List<AnnotationResult> results = buffer.DeserializeFromBinary(variant => 
                    {
                        if (range == null)
                            return true;

                        return variant.Chromosome == range.Chromosome && range.StartPosition <= variant.Position && variant.Position <= range.StopPosition;
                    }
                    ).ToList();

                    foreach (AnnotationResult record in results)
                    {
                        _annotation.Add(record);
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
