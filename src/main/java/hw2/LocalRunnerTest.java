package hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class LocalRunnerTest {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "word count");
        job1.setJarByClass(CooccurringPairs.class);
        /*job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);*/

        String input = args[0];
        String output = args[1];

        int window = Integer.valueOf(args[2]);
        // NINEL - add window config for Mapper as well
        job1.getConfiguration().setInt("window", window);

        Path inputPath = new Path(input);
        Path outputPath = new Path(output);
        Path outputTmpPath = new Path(output + "_temp");

        FileSystem.get(conf).delete(outputTmpPath, true);
        FileSystem.get(conf).delete(outputPath, true);


        FileInputFormat.setInputPaths(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, outputTmpPath);


        job1.setMapOutputKeyClass(TermPair.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(TermPair.class);
        job1.setOutputValueClass(PairOfFloatInt.class);

        job1.setOutputFormatClass(TextOutputFormat.class);

        job1.setMapperClass(CooccurringPairs.PairMapper.class);
        job1.setCombinerClass(CooccurringPairs.PairCombiner.class);
        job1.setReducerClass(CooccurringPairs.PairReducer.class);

        // Delete the output directory if it exists already.
        Path outputDir = new Path("temp");
        FileSystem.get(conf).delete(outputDir, true);

        long startTime = System.currentTimeMillis();
        job1.waitForCompletion(true);
        System.out.println("Job1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        long lineCount = job1.getCounters().findCounter(CooccurringPairs.LINE.count).getValue();

        System.out.println("LINE COUNT: "+lineCount);


        Job job2 = Job.getInstance(conf);
        job2.setJobName(CooccurringPairs.class.getSimpleName());
        job2.setJarByClass(CooccurringPairs.class);

        job2.getConfiguration().setInt("window", window);
        job2.getConfiguration().setInt("lineCount", (int) lineCount);


        FileInputFormat.setInputPaths(job2, outputTmpPath);
        FileOutputFormat.setOutputPath(job2, outputPath);
        //FileSystem.get(conf).delete(outputPath, true);


        job2.setMapOutputKeyClass(TermPair.class);
        job2.setMapOutputValueClass(PairOfFloatInt.class);
        job2.setOutputKeyClass(TermPair.class);
        job2.setOutputValueClass(PairOfFloatInt.class);

        job2.setOutputFormatClass(TextOutputFormat.class);

        job2.setMapperClass(CooccurringPairs.PMIMapper.class);
        job2.setReducerClass(CooccurringPairs.PMIReducer.class);
        job2.setPartitionerClass(CooccurringPairs.MyPartitioner.class);

        startTime = System.currentTimeMillis();
        Boolean waitForCompletion = job2.waitForCompletion(true);
        System.out.println("Job2 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

        System.exit(waitForCompletion ? 0 : 1);
    }

}
