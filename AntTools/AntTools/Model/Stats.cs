using System.Collections.Generic;

using Illumina.Annotator.Model;

namespace Illumina.AntTools.Model
{
    public class Stats
    {
        public string DatasetVersion { get; set; }

        public TranscriptSource TranscriptSource { get; set; }

        public int VariantCount { get; set; }

        public List<ChrRange> Ranges { get; set; }
    }
}
