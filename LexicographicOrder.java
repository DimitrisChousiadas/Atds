import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Character;
import java.util.HashMap;
import java.lang.Long;
import java.util.*;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

public class LexicographicOrder {

    // TODO: to lower case

  public static class PartitionMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "_");
      while (itr.hasMoreTokens()) {
          String keyword = itr.nextToken();
          word.set(keyword);
          context.write(word, new IntWritable(1));
      }
    }
  }

  public static class PartitionCombiner
       extends Reducer<Text, IntWritable, Text, IntWritable> {

    private Text word = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //for (IntWritable val : values) {
          context.write(key, new IntWritable(1));
      //}
    }
  }

  public static class PartitionReducer
       extends Reducer<Text, IntWritable, Text, Text> {

    private Text word = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //for (IntWritable val : values) {
          context.write(key, new Text(""));
      //}
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcounttweaked <in> [<in>...] <out>");
      System.exit(2);
    }

    Job firstJob = new Job(conf, "first Job");
    firstJob.setJarByClass(LexicographicOrder.class);
    Path firstInputPath = new Path(otherArgs[0]);
    Path firstOutputPath = new Path(otherArgs[1]);
    firstJob.setMapperClass(PartitionMapper.class);
    firstJob.setCombinerClass(PartitionCombiner.class);
    firstJob.setReducerClass(PartitionReducer.class);
    firstJob.setOutputKeyClass(Text.class);
    firstJob.setOutputValueClass(IntWritable.class);
    //firstJob.setNumReduceTasks(1);
    FileInputFormat.addInputPath(firstJob, firstInputPath);
    FileOutputFormat.setOutputPath(firstJob, firstOutputPath);
    firstJob.waitForCompletion(true);

    // Create job and parse CLI parameters
    Job job = new Job(conf, "hexicographical order");
    job.setJarByClass(LexicographicOrder.class);

    Path inputPath = firstOutputPath;
    Path partitionOutputPath = new Path(otherArgs[2]);
    //Path partitionOutputPath = new Path("/user/partition10");
    Path outputPath = new Path(otherArgs[3]);

    // The following instructions should be executed before writing the partition file
    job.setNumReduceTasks(10);
    FileInputFormat.setInputPaths(job, inputPath);
    TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionOutputPath);
    job.setInputFormatClass(KeyValueTextInputFormat.class);
    job.setMapOutputKeyClass(Text.class);
    //job.setOutputKeyClass(Text.class);
    //job.setOutputValueClass(IntWritable.class);

    // Write partition file with random sampler
    InputSampler.Sampler<Text, Text> sampler = new InputSampler.RandomSampler<>(0.01, 100, 10);
    InputSampler.writePartitionFile(job, sampler);

    // Use TotalOrderPartitioner and default identity mapper and reducer
    job.setPartitionerClass(TotalOrderPartitioner.class);
    job.setMapperClass(Mapper.class);
    job.setReducerClass(Reducer.class);

    FileOutputFormat.setOutputPath(job, outputPath);
    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}
