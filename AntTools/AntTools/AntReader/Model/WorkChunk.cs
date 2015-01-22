using System.Collections.Generic;

using Illumina.AntTools.Model;

namespace Illumina.AntTools
{
    internal class WorkChunk
    {
        public int Id { get; set; }

        public byte[] Data { get; set; }

        public List<ChrRange> Ranges { get; set; }

        public WorkChunk(int id, byte[] data, List<ChrRange> ranges)
        {
            Id = id;
            Data = data;
            Ranges = ranges;
        }
    }
}
