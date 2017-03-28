import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Character;
import java.util.Collections;
import java.util.ArrayList;
import java.lang.Long;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Sampler {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text dummyKey = new Text("dummy");
    private Text word = new Text();

    @Override
    public void run(Context context) throws IOException, InterruptedException {
      setup(context);
      int count = 0;
      while (context.nextKeyValue() && count++ < 10) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
      }
      System.out.println("Finished" + count);
      cleanup(context);
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "_");
      while (itr.hasMoreTokens()) {
          String keyword = itr.nextToken();
          word.set(keyword);
          context.write(dummyKey, word);
      }
    }
  }

  public static class SamplingReducer
       extends Reducer<Text, Text, Text, Text> {

    private Text word = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      ArrayList<String> ls = new ArrayList<String>();

      int total = 0;
      int stride;
      String keyword;

      for (Text val : values) {
          keyword = val.toString();
          ls.add(keyword);
          total++;
      }

      Collections.sort(ls);
      stride = total/10;

      for (int i = stride; i < total; i = i + stride) {
          context.write(new Text(ls.get(i)), new Text(ls.get(i)));
      }

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcounttweaked <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count tweaked");
    job.setJarByClass(Sampler.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(SamplingReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
