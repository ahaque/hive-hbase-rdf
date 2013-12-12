import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 Written for use on Hadoop 1.x Systems but is compatible with Hadoop 2.x systems
 @author Albert Haque
 @date November 2013
*/

public class AnalyzeTriples {
	
	@SuppressWarnings("rawtypes")
	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		
		private final static IntWritable one = new IntWritable(1);
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			context.write(new Text(value.toString().split(" ")[0]), one);
		}
	}

	 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

		    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
		      throws IOException, InterruptedException {
		        int sum = 0;
		        for (IntWritable val : values) {
		            sum += val.get();
		        }
		        context.write(key, new IntWritable(sum));
		    }
		 }

	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		        
		    Job job = new Job(conf, "AnalyzeTriples-CountIncidence");
		    job.setJarByClass(AnalyzeTriples.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		        
		    job.setMapperClass(Map.class);
		    job.setReducerClass(Reduce.class);
		        
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		        
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		        
		    job.waitForCompletion(true);
		 }



}