using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Illumina.Annotator.Model;

namespace Illumina.AntTools
{
    public class AntWriter : IDisposable
    {
        private const byte AntFormatNumber = 3;

        private readonly BinaryWriter _antWriter;
        private readonly StreamWriter _indexWriter;

        public AntWriter(AnnotationCollection annotationCollection, string outputPath)
        {
            string outputDirectory = Path.GetDirectoryName(outputPath);
            if (!String.IsNullOrEmpty(outputDirectory))
                Directory.CreateDirectory(outputDirectory);

            // this is safe, as BinaryWriter.Dispose() will dispose the Stream
            _antWriter = new BinaryWriter(new FileStream(outputPath, FileMode.Create, FileAccess.ReadWrite));
            _indexWriter = new StreamWriter(new FileStream(String.Format("{0}.idx", outputPath), FileMode.Create, FileAccess.ReadWrite));

            WriteHeader(annotationCollection);
        }

        public void WriteAnnotationChunk(IEnumerable<AnnotationResult> annotation)
        {
            if (annotation == null || !annotation.Any())
                throw new ArgumentException("Invalid annotation chunk.");

            Variant firstVariant = annotation.First().Variant;

            _indexWriter.WriteLine("{0}\t{1}\t{2}", firstVariant.Chromosome, firstVariant.Position, _antWriter.BaseStream.Position);

            byte[] chunkData = BinarySerialization.SerializeToBinary(annotation.ToList());
            //byte[] chunkData = BinaryAnnotationSerializer.SerializeToBinary(annotation.ToList());

            _antWriter.Write(chunkData.Length);
            _antWriter.Write(chunkData);
        }

        public void UpdateChecksums(byte[] vcfHash, byte[] gvcfHash)
        {
            long initialStreamPosition = _antWriter.BaseStream.Position;

            _antWriter.BaseStream.Position = 8; // MAGIC NUMBER: (3 + 1 + 4 = 8)

            _antWriter.Write(vcfHash);
            _antWriter.Write(gvcfHash);

            _antWriter.BaseStream.Position = initialStreamPosition;
        }

        private void WriteHeader(AnnotationCollection annotationCollection)
        {
            _antWriter.Write("ANT".ToArray());          //      03 bytes
            _antWriter.Write(AntFormatNumber);          //      01 byte
            _antWriter.Write(annotationCollection.Id);  //      04 bytes
            _antWriter.Write(new byte[16]);             //      16 bytes
            _antWriter.Write(new byte[16]);             //   +  16 bytes 
                                                        //   =  40 bytes total
        }

        #region IDisposable Members

        protected bool _disposed = false;

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _antWriter.Dispose();

                    _indexWriter.Dispose();
                }

                _disposed = true;
            }
        }

        #endregion
    }
}
