import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Transformer extends Configured implements Tool {

  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    @SuppressWarnings("unchecked")
	@Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
      DataSetProcessor.Triple triple = DataSetProcessor.process(value.toString());
      context.write(new Text(triple.subject), new Text(triple.subject));
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    int res = ToolRunner.run(conf, new Transformer(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {

    // Arguments: <input path> <output path>

    Job job = new Job(getConf());

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.setJobName("KeyExtractor");
    job.setJarByClass(Transformer.class);
    job.setMapperClass(Map.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    job.setInputFormatClass(TextInputFormat.class);
    return job.waitForCompletion(true) ? 0 : 1;

  }
}
