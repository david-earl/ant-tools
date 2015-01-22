using System;
using System.Collections.Generic;
using System.IO;

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

        /// <summary>
        /// Parses an Illumina binary annotation index (.ant.idx).
        /// </summary>
        /// <param name="filepath">A fully qualified path to the index file.</param>
        /// <returns>An array of AntIndex objects.</returns>
        public static AntIndex[] Parse(string filepath)
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
    }
}
