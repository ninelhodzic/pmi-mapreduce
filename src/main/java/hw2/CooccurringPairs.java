package hw2;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.lang.Math;
import java.lang.Float;
import java.lang.Integer;
import java.util.Iterator;
import java.util.*;
import java.io.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CooccurringPairs extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(CooccurringPairs.class);

    public static enum LINE {
        count
    }

    public static final class PairMapper extends Mapper<LongWritable, Text, TermPair, IntWritable> {
        private static final IntWritable ONE = new IntWritable(1);
        private static final TermPair PAIR = new TermPair();
        int window = 2;

        @Override
        public void setup(Context context) {
          //  window = context.getConfiguration().getInt("window", 2);
        }

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            context.getCounter(LINE.count).increment(1);

            // tokenize the input text
            Set<String> uniqueTokens = new HashSet<String>(Tokenizer.tokenize(value.toString()));
            List<String> tokens = new ArrayList<String>();
            tokens.addAll(uniqueTokens);
            	System.out.println(value.toString());
            	System.out.println(tokens);
            	System.out.println(uniqueTokens);


            // count the co-occurrence of pairs
            // NINEL changed - tokens.size()-1 (Argument exception as
            for (int i = 0; i < tokens.size(); i++) {
                for (int j = 0; j < i + window - 1; j++) {
                    if (i == j) continue;
                    String l = tokens.get(i);
                    String r = tokens.get(j);
                    PAIR.set(l,r);
                    context.write(PAIR, ONE);
                    System.out.println("Normal pairs " + PAIR + "Normal ONE " + ONE);
                }
                PAIR.set(tokens.get(i), "*");
                context.write(PAIR, ONE);
                System.out.println("Unique pairs " + PAIR + "Unique ONE " + ONE);
            }

            System.out.println("Mapper: DONE");
        }
    }

    public static final class PairCombiner extends Reducer<TermPair, IntWritable, TermPair, IntWritable> {
        private static final IntWritable SUM = new IntWritable(1);

        @Override
        public void reduce(TermPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            SUM.set(sum);
            context.write(key, SUM);
            System.out.println("Combiner key " + key + "combiner sum " + SUM);
        }
    }

    public static final class PairReducer extends Reducer<TermPair, IntWritable, TermPair, PairOfFloatInt> {
        private static final TermPair KEY = new TermPair();
        private static final PairOfFloatInt VALUE = new PairOfFloatInt(0.0f, 1);
        private float marginal = 0.0f;
        private int window = 1;

        @Override
        public void setup(Context context) {
            window = context.getConfiguration().getInt("window", 1);
        }

        @Override
        public void reduce(TermPair key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0.0f;
            Iterator<IntWritable> iter = values.iterator();
            while (iter.hasNext()) {
                sum += iter.next().get();
            }
            if (key.getRight().equals("*")) {
                VALUE.set(1.0f, (int) sum);
                context.write(key, VALUE);
                marginal = sum;
            } else {
                if (sum >= window) {
                    KEY.set(key.getRight(), key.getLeft());
                    VALUE.set((float) sum / marginal, (int) sum);
                    context.write(KEY, VALUE);
                }
            }
            System.out.println("Reducer: DONE");
        }
    }

    public static final class PMIMapper extends Mapper<LongWritable, Text, TermPair, PairOfFloatInt> {
        private static final TermPair PAIR = new TermPair();
        private static final PairOfFloatInt VALUE = new PairOfFloatInt();

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // parsing the data (A, B) (float, int)
            List<String> tokens = new ArrayList<>();
            String[] splits = value.toString().split("\\s+|\\(|,\\s+|\\)|\r\n|\r|\n");
            for (int i = 0; i < splits.length; i++) {
                if (splits[i].length() != 0) {
                    tokens.add(splits[i]);
                    System.out.println(tokens);
                }
            }

            if (tokens.size() == 4) {
                PAIR.set(tokens.get(0), tokens.get(1));
                VALUE.set(Float.parseFloat(tokens.get(2)), Integer.parseInt(tokens.get(3)));
                context.write(PAIR, VALUE);
                System.out.println(PAIR);
                System.out.println(VALUE);
            }
        }
    }

    public static final class PMIReducer extends Reducer<TermPair, PairOfFloatInt, TermPair, PairOfFloatInt> {
        private static final TermPair KEY = new TermPair();
        private static final PairOfFloatInt VALUE = new PairOfFloatInt();
        private float marginal = 0.0f;
        private int window = 1;
        private int lineCount = 1;

        @Override
        public void setup(Context context) {
            window = context.getConfiguration().getInt("window", 1);
            lineCount = context.getConfiguration().getInt("lineCount", 1);
        }

        @Override
        public void reduce(TermPair key, Iterable<PairOfFloatInt> values, Context context)
                throws IOException, InterruptedException {
            float sum = 0.0f;
            float pmi = 0.0f;
            Iterator<PairOfFloatInt> iter = values.iterator();
            while (iter.hasNext()) {
                PairOfFloatInt temp = iter.next();
                sum += temp.getRight();
                pmi += temp.getLeft();
            }
            System.out.println(sum);
            System.out.println(pmi);

            if (key.getRight().equals("*")) {
                marginal = sum;
            } else {
                if (sum >= window) {
                    VALUE.set((float) Math.log10(lineCount * pmi / marginal), (int) sum);
                    context.write(key, VALUE);
                }
            }
        }
    }

    public static final class MyPartitioner extends Partitioner<TermPair, PairOfFloatInt> {
        @Override
        public int getPartition(TermPair key, PairOfFloatInt value, int numReduceTasks) {
            return (key.getLeft().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }


    public CooccurringPairs() {
    }

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String WINDOW = "window";

    @SuppressWarnings({"static-access"})

    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("W number").create(WINDOW));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String input = cmdline.getOptionValue(INPUT);
        String output = cmdline.getOptionValue(OUTPUT);
        int window = cmdline.hasOption(WINDOW) ? Integer.parseInt(cmdline.getOptionValue(WINDOW)) : 1;

        LOG.info("Tool: " + CooccurringPairs.class.getSimpleName());
        LOG.info(" - input path: " + input);
        LOG.info(" - output path: " + output);
        LOG.info(" - number lines for window: " + window);

        Job job1 = Job.getInstance(getConf());
        job1.setJobName(CooccurringPairs.class.getSimpleName());
        job1.setJarByClass(CooccurringPairs.class);

        FileInputFormat.setInputPaths(job1, new Path(input));
        FileOutputFormat.setOutputPath(job1, new Path(output + "_temp"));

        job1.setMapOutputKeyClass(TermPair.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(TermPair.class);
        job1.setOutputValueClass(PairOfFloatInt.class);

        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setMapperClass(PairMapper.class);
        job1.setCombinerClass(PairCombiner.class);
        job1.setReducerClass(PairReducer.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path("temp");
        FileSystem.get(getConf()).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job1.waitForCompletion(true);
        System.out.println("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        long lineCount = job1.getCounters().findCounter(CooccurringPairs.LINE.count).getValue();

        Job job2 = Job.getInstance(getConf());
        job2.setJobName(CooccurringPairs.class.getSimpleName());
        job2.setJarByClass(CooccurringPairs.class);

        job2.getConfiguration().setInt("window", window);
        job2.getConfiguration().setInt("lineCount", (int) lineCount);

        FileInputFormat.setInputPaths(job2, new Path(output + "_temp"));
        FileOutputFormat.setOutputPath(job2, new Path(output));

        job2.setMapOutputKeyClass(TermPair.class);
        job2.setMapOutputValueClass(PairOfFloatInt.class);
        job2.setOutputKeyClass(TermPair.class);
        job2.setOutputValueClass(PairOfFloatInt.class);

        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapperClass(PMIMapper.class);
        job2.setReducerClass(PMIReducer.class);
        job2.setPartitionerClass(MyPartitioner.class);

        startTime = System.currentTimeMillis();
        job2.waitForCompletion(true);
        System.out.println("Job2 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        return 0;
    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new CooccurringPairs(), args);
    }
}
