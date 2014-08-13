using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Illumina.AntTools.Model;

namespace Illumina.AntTools
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("usage:");

                Console.WriteLine("AnnotationWriter [antFileName]\n\r");

                Console.WriteLine("antFileName: the fully qualified path to the .ant file.");

                return;
            }

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

            Console.Write("Parsing .ant file: ");

            int cursorRow = Console.CursorTop;
            int cursorColumn = Console.CursorLeft;

            int collectionId;

            AntReader reader = new AntReader();

            int count = 0;
            int badCount = 0;

            Stopwatch sw = new Stopwatch();

            sw.Start();

            ChrRange range = null;// new ChrRange() { Chromosome = "chr1", StartPosition = 10000000, StopPosition = 20000000 };

            foreach (AnnotationResult record in reader.Load(antPath, out collectionId, range))
            {
                if (record.Annotation.Any())
                    count++;
                else
                    badCount++;
            }

            sw.Stop();

            Console.WriteLine("\n\rexecution finished.");
        }


        public static void WriteDelimitedResults(string path, IEnumerable<AnnotationResult> results, string delimiterFormat)
        {
            char delimiter = ',';

            if (delimiterFormat.Equals("csv"))
                delimiter = ',';
            else if (delimiterFormat.Equals("tsv"))
                delimiter = '\t';

            try
            {
                using (System.IO.StreamWriter file = new System.IO.StreamWriter(path))
                {
                    foreach (AnnotationResult result in results.Where(p => p.Annotation != null && p.Annotation.Any()))
                    {
                        file.WriteLine(String.Format("{0}{1}{2}{3}{4}{5}{6}",
                            result.Variant.Chromosome, delimiter,
                            result.Variant.Position, delimiter,
                            result.Variant.ReferenceAllele, delimiter,
                            result.Variant.VariantAlleles));


                        foreach (var group in result.Annotation)
                        {
                            file.WriteLine(String.Format("\n{0}{1}", delimiter, group.Key));

                            foreach (var entry in group.Value)
                            {
                                file.WriteLine("{0}{1}{2}", delimiter, delimiter, entry.Key);

                                foreach (var annotation in entry.Value)
                                {
                                    file.WriteLine("{0}{1}{2}{3}{4}{5}", delimiter, delimiter, delimiter, annotation.Key, delimiter, annotation.Value);
                                }
                            }
                        }
                        file.WriteLine("");
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error writing output file: " + e.Message);
            }
        }
    }
}
