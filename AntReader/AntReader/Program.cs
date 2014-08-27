using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

using Mono.Options;

using ServiceStack.Text;

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
            ChrRange range = null;
            int limit = 0;
            bool isBedOutput = false;

            OptionSet options = new OptionSet()
                .Add("validate", dontCare => doValidate = true)
                .Add("stats", dontCare => doGenerateStats = true)
                .Add("range=", chrRange =>
                {
                    if (String.IsNullOrEmpty(chrRange))
                        throw new ArgumentException("Invalid range value given.");

                    range = ParseChrRange(chrRange);
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

            AntReader reader = new AntReader(antPath);

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

            int collectionId;
            int recordCount = 0;

            foreach (AnnotationResult record in reader.Load(out collectionId, range))
            {
                if (!record.Annotation.Any())
                    continue;

                Console.WriteLine(record.ToJson());

                if (limit > 0 && recordCount++ >= limit)
                    break;
            }

            if (recordCount == 0)
                Console.WriteLine("No results.");

            sw.Stop();
        }

        private static void PrintUsage()
        {
            Console.WriteLine("usage:");
            
            Console.WriteLine("AnnotationWriter antFileName [--validate] [--stats] [--range RANGE] [--bed]\n\r");

            Console.WriteLine("antFileName: the fully qualified path to the .ant file.");

            Console.WriteLine("\n\rOptions:");

            Console.WriteLine("\t--validate: validates that the ANT file is in the correct structure.");

            Console.WriteLine("\t--stats: provides a summary of annotation version, contents, etc.");

            Console.WriteLine("\t--range RANGE: allows the specification of a range over which to dump annotations, where RANGE is: CHR:START-STOP.");

            Console.WriteLine("\t--bed: specifies that the output should be BED-like in format, i.e. CHROM, START, STOP, {JSON_DATA}");
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
}
