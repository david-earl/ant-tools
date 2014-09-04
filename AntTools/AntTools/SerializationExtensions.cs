using System;
using System.Collections.Generic;
using System.Linq;

using Illumina.Annotator.Model;

namespace Illumina.AntTools 
{
    public static class SerializationExtensions
    {
        public static byte[] SerializeToBinary(this IEnumerable<AnnotationResult> annotation)
        {
            return BinarySerialization.SerializeToBinary(annotation.ToList());
        }

        public static IEnumerable<AnnotationResult> DeserializeFromBinary(this byte[] data, Func<Variant, bool> variantPredicate = null)
        {
            return BinarySerialization.DeSerializeFromBinary(data, variantPredicate);
        }
    }
}
