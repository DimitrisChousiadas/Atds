import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by foivos on 28/3/2017.
 */
public class Lexicon extends Configured implements Tool{

//    public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {
//
//    }
//
//    public static class FloatSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
//
//    }

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new Lexicon(), args);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: Lexicon <in> [<in>...] <part_out> <out>");
            System.exit(2);
        }

        //create job
        Job job = Job.getInstance(conf, "Lexicon");
        job.setJarByClass(Lexicon.class);
        for (int i = 0; i < otherArgs.length - 2; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        Path partition_output = new Path(otherArgs[otherArgs.length - 2]);
        Path output = new Path(otherArgs[otherArgs.length - 1]);

        //Partition File prep
        job.setNumReduceTasks(10);
        TotalOrderPartitioner.setPartitionFile(job.getConfiguration(),partition_output);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);

        //Sampling
        InputSampler.Sampler<Text,Text> sampler = new InputSampler.RandomSampler<>(0.01, 1000, 100);
        InputSampler.writePartitionFile(job, sampler);

        //Total Ordering
        job.setPartitionerClass(TotalOrderPartitioner.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);

        //Output
        FileOutputFormat.setOutputPath(job, output);
        boolean result = job.waitForCompletion(true);

        // Extras

        return (result ? 0 : 1);
    }
}

