import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Integer;
import java.lang.Long;
import java.util.Collection;
import java.util.Iterator;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.util.GenericOptionsParser;

public class WikiAol {

    // TODO: all keys to lower case

  public static class WikiMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString(), "_");
        while (itr.hasMoreTokens()) {
          word.set(itr.nextToken());
          context.write(word, new Text("wiki"));
        }
    }
  }

  public static class AolMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text keyword = new Text();
    private Text queryId = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

        StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
        String userId = itr.nextToken();
        String keywords = itr.nextToken();
        String timestamp = itr.nextToken();
        StringTokenizer foo = new StringTokenizer(timestamp);
        timestamp = foo.nextToken() + "T" + foo.nextToken();
        queryId.set(userId + ":" + timestamp);
        StringTokenizer keys = new StringTokenizer(keywords);
        while (keys.hasMoreTokens()) {
          keyword.set(keys.nextToken());
          context.write(keyword, queryId);
        }
    }
  }

  public static class FoundReducer
       extends Reducer<Text,Text,Text,Text> {

    public static Text found = new Text("found");
    public static Text notFound = new Text("notFound");

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      boolean flag = false;
      Collection<String> copied = new ArrayList<String>();
      for (Text val : values) {
        copied.add(val.toString());
        if ((new String("wiki")).equals(val.toString())) {
            flag = true;
        }
      }

      Iterator<String> copiedItr = copied.iterator();
      String queryId;
      while (copiedItr.hasNext()) {
          queryId = copiedItr.next();
          if (!((new String("wiki")).equals(queryId))) {
              if (flag) {
                  context.write(new Text(queryId), found);
              } else {
                  context.write(new Text(queryId), notFound);
              }
          }
      }
    }
  }

    public static class DummyMapper
         extends Mapper<Object, Text, Text, Text>{

      private Text word = new Text();

      public void map(Object key, Text value, Context context
                      ) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        String queryId = itr.nextToken();
        String f = itr.nextToken();
        context.write(new Text(queryId), new Text(f));
      }
    }

    public static class UniqueFoundReducer
         extends Reducer<Text, Text, Text, IntWritable> {

      public static IntWritable one = new IntWritable(1);
      public static IntWritable zero = new IntWritable(0);
      public static Text dummyKey = new Text("dummy");

      public void reduce(Text queryId, Iterable<Text> foundVals,
                         Context context
                         ) throws IOException, InterruptedException {

        boolean flag = false;
        for (Text f : foundVals) {
            if ((new String("found")).equals(f.toString())) {
                flag = true;
                break;
            }
        }
        if (flag) {
            context.write(dummyKey, one);
        } else {
            context.write(dummyKey, zero);
        }
      }
  }

      public static class ForwardingMapper
           extends Mapper<Object, Text, Text, IntWritable> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
          StringTokenizer itr = new StringTokenizer(value.toString());
          String dummy = itr.nextToken();
          int val = Integer.parseInt(itr.nextToken());
          context.write(new Text("dummy"), new IntWritable(val));
        }
      }

      public static class SumReducer
           extends Reducer<Text, IntWritable, Text, Text> {

        public static Text ex = new Text("Exists");
        public static Text notEx = new Text("Not exists");

        public void reduce(Text dummy, Iterable<IntWritable> vals,
                           Context context
                           ) throws IOException, InterruptedException {

          long exists = 0;
          long notExists = 0;
          for (IntWritable val : vals) {
              if (val.get() == 0) {
                  notExists++;
              } else {
                  exists++;
              }
        }

        float existsPer = ((float)(exists))/((float)(exists + notExists));
        float notExistsPer = ((float)(notExists))/((float)(exists + notExists));

        context.write(ex, new Text(exists + ", " + existsPer));
        context.write(notEx, new Text(notExists + ", " + notExistsPer));
    }

  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcounttweaked <in> [<in>...] <out>");
      System.exit(2);
    }

    Job job1 = new Job(conf, "word count tweaked");
    Job job2 = new Job(conf, "word count tweaked");
    Job job3 = new Job(conf, "fhas");

    job1.setJarByClass(WikiAol.class);
    MultipleInputs.addInputPath(job1, new Path(otherArgs[0]), TextInputFormat.class, WikiMapper.class);
    MultipleInputs.addInputPath(job1, new Path(otherArgs[1]), TextInputFormat.class, AolMapper.class);
    job1.setReducerClass(FoundReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(Text.class);
    //String p = "/intermediateOutput1";
    FileOutputFormat.setOutputPath(job1, new Path(otherArgs[2]));
    job1.waitForCompletion(true);

    job2.setJarByClass(WikiAol.class);
    job2.setMapperClass(DummyMapper.class);
    job2.setReducerClass(UniqueFoundReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job2, new Path(otherArgs[2]));
    //String p2 = "/intermediateOutput2";
    FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));
    job2.waitForCompletion(true);

    job3.setJarByClass(WikiAol.class);
    job3.setMapperClass(ForwardingMapper.class);
    job3.setReducerClass(SumReducer.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(IntWritable.class);
    job3.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job3, new Path(otherArgs[3]));
    FileOutputFormat.setOutputPath(job3, new Path(otherArgs[4]));
    System.exit(job3.waitForCompletion(true) ? 0 : 1);
  }
}
