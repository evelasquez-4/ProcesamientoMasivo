package org.mdp.hadoop.cli;

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
import org.apache.hadoop.util.GenericOptionsParser;

public class SumPairs {
	public static String SPLIT_REGEX = "\\t";
	
	public static class SumPairsMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		public void map(Object key, Text value, Context output)
						throws IOException, InterruptedException {
			String line = value.toString();
			String[] rawWords = line.split(SPLIT_REGEX);
						
			if(!rawWords[0].isEmpty()) {
				word.set(rawWords[0]+"##"+rawWords[1]);
				output.write(word, one);
			}
		}
		 
	}
	
	public static class SumPairsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context output) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable value: values) {
				sum += value.get();
			}
			output.write(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: "+CountWords.class.getName()+" <in> <out>");
			System.exit(2);
		}
		String inputLocation = otherArgs[0];
		String outputLocation = otherArgs[1];
		
		Job job = Job.getInstance(new Configuration());
	     
	    FileInputFormat.setInputPaths(job, new Path(inputLocation));
	    FileOutputFormat.setOutputPath(job, new Path(outputLocation));
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    
	    job.setMapperClass(SumPairsMapper.class);
	    job.setReducerClass(SumPairsReducer.class);
	     
	    job.setJarByClass(SumPairs.class);
		job.waitForCompletion(true);
	}	

}
