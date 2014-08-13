﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

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
                Console.WriteLine("usage:");

                Console.WriteLine("AnnotationWriter antFileName [--validate] [--stats] [--range RANGE] [--bed]\n\r");

                Console.WriteLine("antFileName: the fully qualified path to the .ant file.");

                Console.WriteLine("\n\rOptions:");

                Console.WriteLine("\t--validate: validates that the ANT file is in the correct structure.");

                Console.WriteLine("\t--stats: provides a summary of annotation version, contents, etc.");

                Console.WriteLine("\t--range RANGE: allows the specification of a range over which to dump annotations, where RANGE is: CHR:START-STOP.");

                Console.WriteLine("\t--bed: specifies that the output should be BED-like in format, i.e. CHROM, START, STOP, {JSON_DATA}");

                return;
            }

            bool doValidate = false;
            bool doGenerateStats = false;
            ChrRange range = null;
            bool isBedOutput = false;

            OptionSet options = new OptionSet()
                .Add("validate", dontCare => doValidate = true)
                .Add("stats", dontCare => doGenerateStats = true)
                .Add("range:", chrRange => range = ParseChrRange(chrRange));

            options.Parse(args);

            string antPath = args[0];

            if (String.IsNullOrEmpty(antPath))
            {
                Console.WriteLine("Invalid arguments.");

                return;
            }

            if (!File.Exists(antPath))
            {
                Console.WriteLine("File not found: {0}", antPath);

                return;
            }

            string indexPath = String.Format("{0}.idx", antPath);

            AntReader reader = new AntReader();

            Stopwatch sw = new Stopwatch();

            sw.Start();

            int collectionId;
            int count = 0;

            foreach (AnnotationResult record in reader.Load(antPath, out collectionId, range))
            {
                if (!record.Annotation.Any())
                    continue;

                count++;

                Console.WriteLine(record.ToJson());
            }

            sw.Stop();
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
