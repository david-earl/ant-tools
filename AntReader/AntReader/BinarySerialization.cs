using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

using Ionic.Zlib;

using Illumina.AntTools.Model;

namespace Illumina.AntTools.Serialization 
{
    public static class BinarySerialization
    {
        public static byte[] SerializeToBinary(List<AnnotationResult> annotation)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                SerializeToStream(ms, annotation);

                ms.Position = 0;
                return ms.ToArray();
            }
        }

        public static void SerializeToStream(Stream stream, List<AnnotationResult> annotation) 
        {
            using (DeflateStream ds = new DeflateStream(stream, CompressionMode.Compress, true))
            using (BinaryWriter writer = new BinaryWriter(ds))
            {
                // write the number of records
                writer.Write((Int32)annotation.Count());

                foreach (AnnotationResult result in annotation)
                {
                    // write the variant
                    WriteVariant(writer, result.Variant);

                    // write the group count
                    writer.Write((ushort)result.Annotation.Count());

                    foreach (var group in result.Annotation)
                    {
                        // write the group name index 
                        writer.Write((ushort)group.Key.ToIndex());

                        // write the number of features
                        writer.Write((ushort)group.Value.Count());

                        foreach (var featureGroup in group.Value)
                        {
                            // write the key
                            writer.Write(featureGroup.Key);

                            // write the number of KV pairs
                            writer.Write((ushort)featureGroup.Value.Count());

                            foreach (var keyVals in featureGroup.Value)
                            {
                                writer.Write(keyVals.Key);
                                writer.Write(keyVals.Value);
                            }
                        }
                    }
                }
            }
        }

        public static IEnumerable<AnnotationResult> DeSerializeFromBinary(byte[] data, Func<Variant, bool> variantPredicate = null)
        {
            using (MemoryStream ms = new MemoryStream(data))
            {
                return DeSerializeFromStream(ms, variantPredicate);
            }
        }

        public static IEnumerable<AnnotationResult> DeSerializeFromStream(Stream stream, Func<Variant, bool> variantPredicate)
        {
            if (variantPredicate == null)
                variantPredicate = (dontCare) => { return true; };

            List<AnnotationResult> result = new List<AnnotationResult>();

            byte[] decompressedBytes = null;

            // deflate first, in deference to mono crashes
            using (MemoryStream ms = new MemoryStream())
            using (DeflateStream ds = new DeflateStream(stream, CompressionMode.Decompress, true))
            {
                ds.CopyTo(ms);
                decompressedBytes = ms.ToArray();
            }

            using (MemoryStream ms = new MemoryStream(decompressedBytes))
            using (BinaryReader reader = new BinaryReader(ms))
            {
                // read the number of records
                int recordCount = reader.ReadInt32();

                for (int recordCounter = 0; recordCounter < recordCount; recordCounter++)
                {
                    Variant variant = ReadVariant(reader);

                    if (!variantPredicate(variant))
                    {
                        // read the group count
                        ushort _groupCount = reader.ReadUInt16();

                        for (int _groupCounter = 0; _groupCounter < _groupCount; _groupCounter++)
                        {
                            // read the group name
                            reader.ReadUInt16();

                            // read the number of features
                            int _featureCount = reader.ReadUInt16();

                            for (int _featureCounter = 0; _featureCounter < _featureCount; _featureCounter++)
                            {
                                // read the key
                                reader.ReadString();

                                // read the number of KV pairs
                                int _keyValCount = reader.ReadUInt16();

                                for (int _keyValCounter = 0; _keyValCounter < _keyValCount; _keyValCounter++)
                                {
                                    reader.ReadString();
                                    reader.ReadString();
                                }
                            }
                        }

                        // TODO: this was the right thing to do for VariantStudio...but I think it's no longer appropriate
                        //result.Add(new AnnotationResult() { Variant = null, Annotation = null });

                        continue;
                    }

                    AnnotationResult record = new AnnotationResult() { Annotation = new Dictionary<AnnotationGroupName, Dictionary<string, Dictionary<string, string>>>() };

                    // read the variant
                    record.Variant = variant;
                    //record.Variant = ReadVariant(reader);

                    // read the group count
                    ushort groupCount = reader.ReadUInt16();

                    for (int groupCounter = 0; groupCounter < groupCount; groupCounter++)
                    {
                        // read the group name
                        ushort groupNameIndex = reader.ReadUInt16();

                        Dictionary<string, Dictionary<string, string>> group = new Dictionary<string, Dictionary<string, string>>();

                        // read the number of features
                        int featureCount = reader.ReadUInt16();

                        for (int featureCounter = 0; featureCounter < featureCount; featureCounter++)
                        {
                            // read the key
                            string featureKey = reader.ReadString();

                            Dictionary<string, string> keyVals = new Dictionary<string, string>();

                            // read the number of KV pairs
                            int keyValCount = reader.ReadUInt16();

                            for (int keyValCounter = 0; keyValCounter < keyValCount; keyValCounter++)
                            {
                                keyVals.Add(String.Intern(reader.ReadString()), String.Intern(reader.ReadString()));
                            }

                            group.Add(featureKey, keyVals);
                        }

                        record.Annotation.Add(groupNameIndex.ToEnum(), group);
                    }

                    result.Add(record);
                }
            }

            return result;
        }

        public static byte[] SerializeAndCompress(List<Variant> variants)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                SerializeVariantsToStream(ms, variants);

                ms.Position = 0;
                return ms.ToArray();
            }
        }

        private static void SerializeVariantsToStream(Stream stream, List<Variant> variants)
        {
            using (DeflateStream ds = new DeflateStream(stream, CompressionMode.Compress, true))
            using (BinaryWriter writer = new BinaryWriter(ds))
            {
                // write the number of variants
                writer.Write(variants.Count());

                foreach (Variant variant in variants)
                {
                    WriteVariant(writer, variant);
                }
            }
        }

        public static IEnumerable<Variant> DeserializeVariants(byte[] data)
        {
            using (MemoryStream ms = new MemoryStream(data))
            {
                return DeserializeVariantsFromStream(ms);
            }
        }

        private static IEnumerable<Variant> DeserializeVariantsFromStream(Stream stream)
        {
            List<Variant> result = new List<Variant>();

            byte[] decompressed = null;

            // deflate first, in deference to BioInfo apps crashing under mono
            using (MemoryStream ms = new MemoryStream())
            using (DeflateStream ds = new DeflateStream(stream, CompressionMode.Decompress, true))
            {
                ds.CopyTo(ms);
                decompressed = ms.ToArray();
            }

            using (MemoryStream ms = new MemoryStream(decompressed))
            using (BinaryReader reader = new BinaryReader(ms))
            {
                int count = reader.ReadInt32();

                for (int i = 0; i < count; i++)
                {
                    result.Add(ReadVariant(reader));
                }
            }

            return result;
        }

        private static void WriteVariant(BinaryWriter writer, Variant variant)
        {
            writer.Write(variant.Chromosome);
            writer.Write((Int64)variant.Position);
            writer.Write(variant.ReferenceAllele);
            writer.Write(variant.VariantAlleles);
        }

        private static Variant ReadVariant(BinaryReader reader)
        {
            return new Variant()
                {
                    Chromosome = String.Intern(reader.ReadString()),
                    Position = reader.ReadInt64(),
                    ReferenceAllele = String.Intern(reader.ReadString()),
                    VariantAlleles = String.Intern(reader.ReadString())
                };
        }
    }

    internal static class RandomExtensions
    {
        internal static readonly AnnotationGroupName[] GroupNameLookup = new AnnotationGroupName[]
                {
                    AnnotationGroupName.Allelic,
                    AnnotationGroupName.ClinVar,
                    AnnotationGroupName.Cosmic,
                    AnnotationGroupName.Ensembl,
                    AnnotationGroupName.Hgmd,
                    AnnotationGroupName.Positional,
                    AnnotationGroupName.RefSeq,
                    AnnotationGroupName.Regulatory 
                };

        public static AnnotationGroupName ToEnum(this ushort index)
        {
            return GroupNameLookup[index];
        }

        public static ushort ToIndex(this AnnotationGroupName groupName)
        {
            return (ushort)Array.IndexOf(GroupNameLookup, groupName);
        }
    }
}
