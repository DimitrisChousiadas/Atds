import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Character;
import java.util.HashMap;
import java.lang.Long;
import java.util.*;

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
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LexicographicOrder {

  public static class TokenizerMapper
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

  public static class WordReducer
       extends Reducer<Text, IntWritable, Text, Text> {

    private Text word = new Text();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      context.write(key, new Text(""));

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcounttweaked <in> [<in>...] <out>");
      System.exit(2);
    }

    Job job = new Job(conf, "hexicographical order");
    job.setJarByClass(LexicographicOrder.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(WordReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setNumReduceTasks(5);
    TotalOrderPartitioner.setPartitionFile(conf, new Path("/user/partition10/part-r-00000"));
    job.setPartitionerClass(TotalOrderPartitioner.class);
    for (int i = 0; i < otherArgs.length - 2; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    job.setInputFormatClass(SequenceFileInputFormat.class);
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}
