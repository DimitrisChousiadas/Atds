import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Long;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Popular {

  public static class TokenizerMapper1
       extends Mapper<Object, Text, Text, LongWritable>{

    private final static LongWritable one = new LongWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

        //StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
        String[] itr = value.toString().split("\t", -1);
        String keywords = itr[1];
        itr = new StringTokenizer(keywords);
        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken().toLowerCase());
          context.write(word, one);
        }
    }
  }

  public static class LongSumReducer1
       extends Reducer<Text,LongWritable,Text,LongWritable> {
    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (LongWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

    public static class TokenizerMapper2
         extends Mapper<Object, Text, LongWritable, Text>{

      private LongWritable result = new LongWritable();
      private Text word = new Text();

      public void map(Object key, Text value, Context context
                      ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        String keyword = itr.nextToken();
        long num = Long.parseLong(itr.nextToken());
        word.set(keyword);
        result.set(num);
        context.write(result, word);
      }
    }

    public static class LongSumReducer2
         extends Reducer<LongWritable, Text, Text, LongWritable> {
      private LongWritable result = new LongWritable();

      public void reduce(LongWritable value, Iterable<Text> keywords,
                         Context context
                         ) throws IOException, InterruptedException {

        for (Text keyword : keywords) {
          context.write(keyword, value);
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

    Job job1 = new Job(conf, "first MapReduce job to find popular keywords");
    Job job2 = new Job(conf, "second MapReduce job to find popular keywords");

    job1.setJarByClass(Popular.class);
    job1.setMapperClass(TokenizerMapper1.class);
    //job1.setCombinerClass(IntSumReducer.class);
    job1.setReducerClass(LongSumReducer1.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(LongWritable.class);

    job2.setJarByClass(Popular.class);
    job2.setMapperClass(TokenizerMapper2.class);
    //job1.setCombinerClass(IntSumReducer.class);
    job2.setReducerClass(LongSumReducer2.class);
    job2.setOutputKeyClass(LongWritable.class);
    job2.setOutputValueClass(Text.class);
    job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
    job2.setNumReduceTasks(1);

    FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[1]));
    job1.waitForCompletion(true);

    FileInputFormat.addInputPath(job2, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}
