using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

using Mono.Options;

using ServiceStack.Text;

using Illumina.Annotator.Model;
using Illumina.AntTools.Model;

namespace Illumina.AntTools
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 1)
            {
                PrintUsage();

                return;
            }

            bool doValidate = false;
            bool doGenerateStats = false;
            bool doIncludeAllResults = false;
            List<ChrRange> ranges = null;
            int limit = 0;
            bool isBedOutput = false;
            int memoryLimit = 0;

            OptionSet options = new OptionSet()
                .Add("validate", dontCare => doValidate = true)
                .Add("stats", dontCare => doGenerateStats = true)
                .Add("bedout", dontCare => isBedOutput = true)
                .Add("range=", chrRange =>
                {
                    if (String.IsNullOrEmpty(chrRange))
                        throw new ArgumentException("Invalid range value given.");

                    if (ranges != null)
                        throw new ArgumentException("Conflicting arguments: 'range' and 'bed' are mutually exclusive.");

                    ranges = new List<ChrRange>() { ParseChrRange(chrRange) };
                })
                .Add("bed=", bedPath =>
                {
                    if (ranges != null)
                        throw new ArgumentException("Conflicting arguments: 'range' and 'bed' are mutually exclusive.");

                    ranges = ParseBed(bedPath);
                })
                .Add("memlimit=", memLimit =>
                {
                    memoryLimit = Convert.ToInt32(memLimit);
                })
                .Add("all", dontCare =>
                {
                    if (ranges != null)
                        throw new ArgumentException("Conflicting arguments: 'all' and 'bed'/'range' are mutually exclusive.");

                    doIncludeAllResults = true;
                })
                .Add("limit=", n => limit = Convert.ToInt32(n));

            options.Parse(args);

            string antPath = args[0];

            if (String.IsNullOrEmpty(antPath))
            {
                Console.WriteLine("Invalid arguments.");

                PrintUsage();

                return;
            } 

            if (!File.Exists(antPath))
            {
                Console.WriteLine("File not found: {0}", antPath);

                return;
            }

            int collectionId;

            AntReader reader = new AntReader(antPath, out collectionId);

            if (memoryLimit > 0)
                reader.MemoryAllocationLimitMb = memoryLimit;

            if (doValidate)
            {
                reader.Validate();

                return;
            }

            if (doGenerateStats)
            {
                reader.PrintAntStats();

                return;
            }

            Stopwatch sw = new Stopwatch();

            sw.Start();

            int recordCount = 0;

                foreach (AnnotationResult record in reader.Load(ranges))
                {
                    recordCount++;

                    if (!record.Annotation.Any() && !doIncludeAllResults)
                        continue;

                    if (limit > 0 && recordCount >= limit)
                        break;

                    Console.WriteLine(isBedOutput ? String.Format("{0}\t{1}\t{2}\t{3}", record.Variant.Chromosome, record.Variant.Position, record.Variant.Position, record.ToJson()) : record.ToJson());
                }

            sw.Stop();

            if (recordCount == 0)
                Console.WriteLine("No results.");
        }

        private static void PrintUsage()
        {
            Console.WriteLine("usage:");
            Console.WriteLine("ant-tools.py antFileName [--validate] [--stats] [--range=RANGE] [--bed=PATH] [--memlimit=LIMIT_IN_MB\n\r");
            Console.WriteLine("antFileName: the fully qualified path to the .ant file.");
            Console.WriteLine("\n\rOptions:");
            Console.WriteLine("\t--validate: validates that the ANT file is in the correct structure.");
            Console.WriteLine("\t--stats: provides a summary of annotation version, contents, etc.");
            Console.WriteLine("\t--all: specifies that empty records (i.e. no annotation) should be included in the output.");
            Console.WriteLine("\t--range=RANGE: allows the specification of a range over which to dump annotations, where RANGE is: CHR:START-STOP.");
            Console.WriteLine("\t--bed=PATH specifies an input BED file indicating the regions for which to export annotation.");
            Console.WriteLine("\t--bedout: specifies that the output should be BED-like in format, i.e. CHROM, START, STOP, {JSON_DATA}");
            Console.WriteLine("\t--memlimit: specifies the maximum amount of memory (in MB) that can be allocated by ant-tools.");
        }

        private static List<ChrRange> ParseBed(string bedFilePath)
        {
            if (!File.Exists(bedFilePath))
                throw new Exception(String.Format("Unable to find the specified BED file: {0}", bedFilePath));

            List<ChrRange> ranges = new List<ChrRange>();

            using (StreamReader reader = new StreamReader(bedFilePath))
            {
                string line;

                while ((line = reader.ReadLine()) != null)
                {
                    if (String.IsNullOrEmpty(line) || line.StartsWith("#"))
                        continue;

                    string[] splits = line.Split('\t');

                    if (splits.Length < 3)
                        continue; // FAIL?

                    ranges.Add(new ChrRange() { Chromosome = splits[0], StartPosition = Convert.ToInt64(splits[1]), StopPosition = Convert.ToInt64(splits[2]) } );
                }
            }

            return ranges;
        }

        private static ChrRange ParseChrRange(string chrRange)
        {
            Regex regex = new Regex(@"(.*):(\d+)-(\d+)");

            Match matches = regex.Match(chrRange);

            if (matches.Groups.Count != 4)
                throw new Exception("Chromosome range was in an incorrect format.");

            return new ChrRange()
            {
                Chromosome = matches.Groups[1].Value,
                StartPosition = Convert.ToInt64(matches.Groups[2].Value),
                StopPosition = Convert.ToInt64(matches.Groups[3].Value)
            };
        }
    }

    internal static class Extensiones
    {
        public static void WriteToStream(this AnnotationResult record, TextWriter stream)
        {
            foreach (var foo in record.Annotation)
            {
                stream.WriteLine(foo.Key);
                
                foreach (var bar in foo.Value)
                {
                    stream.WriteLine(bar.Key);

                    foreach (var baz in bar.Value)
                    {
                        stream.WriteLine("{0}:{1}", baz.Key, baz.Value);
                    }
                }
            }

            stream.Flush();
        }
    }
}
