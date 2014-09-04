using System;

namespace Illumina.AntTools.Model 
{
    public class AntIndex
    {
        public string Chromosome { get; set; }

        public long ChrPosition { get; set; }

        public long FilePosition { get; set; }

        public AntIndex(string chr, string pos, string filePos)
        {
            Chromosome = chr;
            ChrPosition = Convert.ToInt64(pos);
            FilePosition = Convert.ToInt64(filePos);
        }
    }
}
