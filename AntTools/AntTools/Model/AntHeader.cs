using System;
using System.IO;
using System.Linq;
using System.Text;

namespace Illumina.AntTools.Model
{
    public class AntHeader
    {
        private static readonly int _headerLength = 40;

        public static readonly byte _antFormatNumber = 3;

        public static string AntVersion
        {
            get { return String.Format("ANT{0}", _antFormatNumber); }
        }

        public static int HeaderLength { get { return _headerLength; } }

        public static int Parse(string filepath)
        {
            int annotationCollectionId;

            using (FileStream stream = File.Open(filepath, FileMode.Open))
            using (BinaryReader reader = new BinaryReader(stream, Encoding.ASCII))
            {
                // read in the header
                byte[] header = new byte[_headerLength];
                if (reader.Read(header, 0, _headerLength) != HeaderLength)
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
