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


public class XmlFileSortTags {
	
	public static class SortTagsCountsMapper extends Mapper<Object, Text, IntWritable,Text>{
		@Override
		public void map(Object key, Text value, Context output) throws IOException, InterruptedException 
		{
			String[] split = value.toString().split("\t");
				
			output.write(new DescendingIntWritable(Integer.parseInt(split[1])),new Text(split[0]));
		}	
	}
	
	public static class SortTagsCountsReducer extends Reducer<DescendingIntWritable,Text, Text, IntWritable> 
	{
		@Override
		public void reduce(DescendingIntWritable key, Iterable<Text> values,Context output) throws IOException, InterruptedException {
				for(Text value:values) {
					output.write(value,key);
				}
			}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		String inputLocation = otherArgs[0];
		String outputLocation = otherArgs[1];

		Job job = Job.getInstance(new Configuration());
		job.setMapOutputKeyClass(DescendingIntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(SortTagsCountsMapper.class);
		job.setReducerClass(SortTagsCountsReducer.class);

		FileInputFormat.setInputPaths(job, new Path(inputLocation));
		FileOutputFormat.setOutputPath(job, new Path(outputLocation));

		job.setJarByClass(XmlFileSortTags.class);
		job.waitForCompletion(true);
	}

	public static class DescendingIntWritable extends IntWritable{
		
		public DescendingIntWritable(){}
		
		public DescendingIntWritable(int val){
			super(val);
		}
		
		public int compareTo(IntWritable o) {
			return -super.compareTo(o);
		}
}
}
