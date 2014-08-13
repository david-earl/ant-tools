using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace Illumina.AntTools.Model
{
    public class ChrRange
    {
        public string Chromosome { get; set; }

        public long StartPosition { get; set; }

        public long StopPosition { get; set; }
    }
}
