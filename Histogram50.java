import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Character;
import java.util.HashMap;
import java.lang.Long;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Histogram50 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private Text dummyKey = new Text("dummy");
    private Text word = new Text();

    public String whatIsTheKey (char c) {
        if (c >= '0' && c <= '9')
            return "number";
        else if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z'))
            return ("" + Character.toUpperCase(c));
        else
            return "symbol";
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
      setup(context);
      int count = 0;
      while (context.nextKeyValue() && count++ < 50) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
      }
      System.out.println("Finished" + count);
      cleanup(context);
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "_");
      while (itr.hasMoreTokens()) {
          String str = itr.nextToken();
          String keyword = whatIsTheKey(str.charAt(0));
          word.set(keyword);
          context.write(dummyKey, word);
      }
    }
  }

  public static class FloatSumReducer
       extends Reducer<Text, Text, Text, FloatWritable> {

    private FloatWritable result = new FloatWritable();
    private Text word = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      long total = 0;
      long temp;
      String keyword;
      HashMap<String, Long> hm = new HashMap<String, Long>();

      hm.put("A", new Long(0));
      hm.put("B", new Long(0));
      hm.put("C", new Long(0));
      hm.put("D", new Long(0));
      hm.put("E", new Long(0));
      hm.put("F", new Long(0));
      hm.put("G", new Long(0));
      hm.put("H", new Long(0));
      hm.put("I", new Long(0));
      hm.put("J", new Long(0));
      hm.put("K", new Long(0));
      hm.put("L", new Long(0));
      hm.put("M", new Long(0));
      hm.put("N", new Long(0));
      hm.put("O", new Long(0));
      hm.put("P", new Long(0));
      hm.put("Q", new Long(0));
      hm.put("R", new Long(0));
      hm.put("S", new Long(0));
      hm.put("T", new Long(0));
      hm.put("U", new Long(0));
      hm.put("V", new Long(0));
      hm.put("W", new Long(0));
      hm.put("X", new Long(0));
      hm.put("Y", new Long(0));
      hm.put("Z", new Long(0));
      hm.put("number", new Long(0));
      hm.put("symbol", new Long(0));

      for (Text val : values) {
          keyword = val.toString();
          temp = hm.remove(keyword).longValue();
          hm.put(keyword, new Long(temp+1));
          total++;
      }

      Iterator it = hm.entrySet().iterator();
      while (it.hasNext()) {
          Map.Entry pair = (Map.Entry)it.next();
          word.set((String)pair.getKey());
          result.set(((float)((Long) pair.getValue()).longValue()) / ((float)total));
          context.write(word, result);
          it.remove(); // avoids a ConcurrentModificationException
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
    job.setJarByClass(Histogram50.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(FloatSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(1);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
