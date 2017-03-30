import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Date;
import java.util.Calendar;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.HashSet;
import java.lang.Integer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Visits10 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
      String[] itr = value.toString().split("\t", -1);
      String url;
      int userId;

      if (itr.length == 5) {

          userId = Integer.parseInt(itr[0]);
          url = itr[4];

          word.set(url);
          context.write(word, new IntWritable(userId));

      }

    }
  }

  public static class IntFilterReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {

      int sum = 0;
      Integer userId;

      HashSet<Integer> hs = new HashSet<Integer>();

      for (IntWritable val : values) {
          userId = new Integer(val.get());
          if (!hs.contains(userId)) {
              sum++;
              hs.add(userId);
          }
      }

      if (sum > 10) {
          result.set(sum);
          context.write(key, result);
      }

    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: visits10 <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "calculates urls with more than 10 visits");
    job.setJarByClass(Visits10.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntFilterReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
