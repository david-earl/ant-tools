using System;
using System.Collections.Generic;

using Illumina.Annotator.Model;

namespace Illumina.AntTools.Model
{
    public static class Chromosome
    {
        private static readonly List<string> _refseqChrs = new List<string>() 
        { 
            "chrM", "chr1", "chr2", "chr3", "chr4", "chr5", "chr6", "chr7", "chr8", "chr9", "chr10", "chr11", "chr12", "chr13", "chr14", "chr15", "chr16", "chr17", "chr18", "chr19", "chr20", "chr21", "chr22", "chrX", "chrY" 
        };

        private static readonly List<string> _ensemblChrs = new List<string>() 
        { 
            "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "20", "21", "22", "X", "Y", "MT" 
        };

        public static readonly Dictionary<TranscriptSource, List<string>> ChrsByTranscriptSource = new Dictionary<TranscriptSource, List<string>>() 
        {
            { TranscriptSource.RefSeq, _refseqChrs },
            { TranscriptSource.Ensembl, _ensemblChrs }
        };

        public static bool IsLessThan(string lhs, string rhs)
        {
            TranscriptSource transcriptSource = InferTranscriptSource(lhs);

            if (transcriptSource != InferTranscriptSource(rhs))
                throw new Exception("Chromosome format mismatch; please ensure any specified ranges use the same chromosome format as the source .ant file.");

            if (!ChrsByTranscriptSource[transcriptSource].Contains(lhs) || !ChrsByTranscriptSource[transcriptSource].Contains(rhs))
                throw new Exception("Unknown chr specified.");

            return ChrsByTranscriptSource[transcriptSource].FindIndex(p => p.Equals(lhs, StringComparison.OrdinalIgnoreCase)) < ChrsByTranscriptSource[transcriptSource].FindIndex(p => p.Equals(rhs, StringComparison.OrdinalIgnoreCase));
        }

        public static bool IsGreaterThan(string lhs, string rhs)
        {
            TranscriptSource transcriptSource = InferTranscriptSource(lhs);

            if (transcriptSource != InferTranscriptSource(rhs))
                throw new Exception("Chromosome format mismatch; please ensure any specified ranges use the same chromosome format as the source .ant file.");

            if (!ChrsByTranscriptSource[transcriptSource].Contains(lhs) || !ChrsByTranscriptSource[transcriptSource].Contains(rhs))
                throw new Exception("Unknown chr specified.");

            return ChrsByTranscriptSource[transcriptSource].FindIndex(p => p.Equals(lhs, StringComparison.OrdinalIgnoreCase)) > ChrsByTranscriptSource[transcriptSource].FindIndex(p => p.Equals(rhs, StringComparison.OrdinalIgnoreCase));
        }

        private static TranscriptSource InferTranscriptSource(string chr)
        {
            return chr.StartsWith("chr") ? TranscriptSource.RefSeq : TranscriptSource.Ensembl;
        }
    }
}
