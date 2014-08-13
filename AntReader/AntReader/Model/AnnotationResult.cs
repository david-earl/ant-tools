using System;
using System.Collections.Generic;

namespace Illumina.AntTools.Model 
{
    [Serializable]
    public class AnnotationResult
    {
        public Variant Variant { get; set; }

        public Dictionary<AnnotationGroupName, Dictionary<string, Dictionary<string, string>>> Annotation { get; set; }
    }

    public enum AnnotationGroupName
    {
        Positional,

        Allelic,

        Hgmd,

        Cosmic,

        RefSeq,

        Ensembl,

        Regulatory,

        ClinVar
    }
}
