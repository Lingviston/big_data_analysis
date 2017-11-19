package edu.gatech.cse6242;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Task1 {
	public static class IncomingNodeMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
	
		private final IntWritable node = new IntWritable();
		
		public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
			final String[] edge = value.toString().split("\t");
			
			final int target = Integer.parseInt(edge[1]);
			final int weight = Integer.parseInt(edge[2]);
			
			if(weight > 0) {
				node.set(target);
				context.write(node, new IntWritable(weight));
			}
		}
	}

	public static class IncomingNodeReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
		private final IntWritable result = new IntWritable();
		
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
			
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		final Job job = Job.getInstance(conf, "Task1");
		job.setJarByClass(Task1.class);
		job.setMapperClass(IncomingNodeMapper.class);
		job.setCombinerClass(IncomingNodeReducer.class);
		job.setReducerClass(IncomingNodeReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
