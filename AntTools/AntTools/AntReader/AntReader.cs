using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using Illumina.Annotator.Model;
using Illumina.AntTools.Model;

namespace Illumina.AntTools
{
    public class AntReader
    {
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

        //private Func<int, Variant, bool> _userVariantPredicate;

        private readonly string _antPath;
        private readonly int _annotationCollectionId;
        private readonly object _statsLock = new object();
        private Stats _antStats;

        public Action<double> ProgressCallback { get; set; }
        public int MemoryAllocationLimitMb { get; set; }

        public AntReader(string filepath, out int annotationCollectionId)
        {
            _antPath = filepath;

            _annotationCollectionId = AntHeader.Parse(_antPath);

            annotationCollectionId = _annotationCollectionId;

            MemoryAllocationLimitMb = 2048;
        }

        /// <summary>
        /// Extracts all annotation from an .ant within the given BED-style ranges
        /// </summary>
        /// <param name="ranges"></param>
        /// <returns></returns>
        public IEnumerable<AnnotationResult> Load(List<ChrRange> ranges, CancellationToken cancellationToken = default (CancellationToken))
        {
            return Load(pc => pc.Start(ranges), cancellationToken);
        }

        /// <summary>
        /// Extracts all annotation from an .ant where the variant passes the given boolean predicate.
        /// </summary>
        /// <param name="variantPredicate"></param>
        /// <returns></returns>
        public IEnumerable<AnnotationResult> Load(Func<int, Variant, bool> variantPredicate, CancellationToken cancellationToken = default (CancellationToken))
        {
            return Load(pc => pc.Start(variantPredicate), cancellationToken);
        }

        private IEnumerable<AnnotationResult> Load(Action<AntProducerConsumer> initAction, CancellationToken cancellationToken)
        {
            AntProducerConsumer producerConsumer = new AntProducerConsumer(_antPath, cancellationToken);

            producerConsumer.ProgressCallback = ProgressCallback;
            producerConsumer.MemoryAllocationLimitMb = MemoryAllocationLimitMb;

            initAction(producerConsumer);

            int recordIndex = 0;

            while (!producerConsumer.IsDone || producerConsumer.Annotation.Any())
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    Console.WriteLine("Yield break.");
                    
                    yield break;
                }

                if (!producerConsumer.Annotation.ContainsKey(recordIndex))
                {
                    Thread.Sleep(10);

                    continue;
                }

                yield return producerConsumer.Annotation[recordIndex];

                AnnotationResult dontCare;
                producerConsumer.Annotation.TryRemove(recordIndex++, out dontCare);
            }
        }

        /// <summary>
        /// Determines if the .ant is structurally valid and not corrupt.
        /// </summary>
        /// <returns></returns>
        public bool Validate()
        {
            Console.Write("Validating ANT file...");

            try
            {
                AntProducerConsumer producerConsumer = new AntProducerConsumer(_antPath);

                producerConsumer.Start((variant, ranges) => VariantPredicate(variant, ranges, false, true));

                while (!producerConsumer.IsDone)
                {
                    Thread.Sleep(10);
                }

                Console.WriteLine("valid.");
            }
            catch (Exception e)
            {
                Console.WriteLine("invalid: {0}.", e.Message);

                return false;
            }

            return true;
        }

        /// <summary>
        /// Displays statistics about the .ant file.
        /// </summary>
        public void PrintAntStats()
        {
            AntProducerConsumer producerConsumer = new AntProducerConsumer(_antPath);

            producerConsumer.Start((variant, ranges) => VariantPredicate(variant, ranges, true, false));

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
            while (!producerConsumer.IsDone)
            {
                if (!producerConsumer.Annotation.ContainsKey(recordIndex))
                {
                    Thread.Sleep(10);

                    continue;
                }

                AnnotationResult dontCare;
                producerConsumer.Annotation.TryRemove(recordIndex++, out dontCare);
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

        private bool VariantPredicate(Variant variant, List<ChrRange> ranges, bool isStatsLoad, bool isValidating)
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

            if (ranges == null || !ranges.Any())
                return true;

            return ranges.Any(p => variant.Chromosome == p.Chromosome && p.StartPosition <= variant.Position && variant.Position <= p.StopPosition);
        }
    }
}
