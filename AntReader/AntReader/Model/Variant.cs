using System;
using System.Runtime.Serialization;

namespace Illumina.AntTools.Model 
{
    [DataContract, Serializable]
    public class Variant : IEquatable<Variant>
    {
        [DataMember(Order = 1)]
        public string Chromosome { get; set; }

        [DataMember(Order = 2)]
        public Int64 Position { get; set; }

        [DataMember(Order = 3)]
        public string ReferenceAllele { get; set; }

        [DataMember(Order = 4)]
        public string VariantAlleles { get; set; }

        [DataMember(Order = 5)]
        public int Id { get; set; }

        public bool Equals(Variant other)
        {
            if (Object.ReferenceEquals(other, null)) 
                return false;

            if (Object.ReferenceEquals(this, other)) 
                return true;

            return Chromosome.Equals(other.Chromosome) && Position.Equals(other.Position) &&
                   ReferenceAllele.Equals(other.ReferenceAllele) && VariantAlleles.Equals(other.VariantAlleles);
        }

        public override int GetHashCode()
        {
            return Chromosome.GetHashCode() ^ Position.GetHashCode() ^ ReferenceAllele.GetHashCode() ^ VariantAlleles.GetHashCode();
        }
    }
}
