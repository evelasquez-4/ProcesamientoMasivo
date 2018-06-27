package org.mdp.hadoop.cli;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mdp.hadoop.cli.SortWordCounts.DescendingIntWritable;


public class SortRatingStarsByAvg {
	
	public static class SortRatingStarsMapper extends Mapper<Object, Text, DoubleWritable, Text>{
		@Override
		public void map(Object key, Text value, Context output)
						throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			output.write(new DescendingDoubleWritable(Double.parseDouble(split[1])),new Text(split[0].toString()));
		}
	}
	
	
	public static class SortRatingStarsReducer 
    extends Reducer<DescendingDoubleWritable, Text,Text,DoubleWritable> {

		@Override
		public void reduce(DescendingDoubleWritable key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			for(Text res:values) {
				output.write(res,null);
			}
		}
	}
	
	
	public static class DescendingDoubleWritable extends DoubleWritable{
		
		public DescendingDoubleWritable(){}
		public DescendingDoubleWritable(double i) {
			super(i);
		}
		
		public int compareTo(DoubleWritable d) {
			//return compareValue;    // sort ascending
			return -super.compareTo(d);   // sort descending
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: "+SortWordCounts.class.getName()+" <in> <out>");
			System.exit(2);
		}
		String inputLocation = otherArgs[0];
		String outputLocation = otherArgs[1];

		Job job = Job.getInstance(new Configuration());
		job.setMapOutputKeyClass(DescendingDoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		job.setMapperClass(SortRatingStarsMapper.class); // no combiner this time!
		job.setReducerClass(SortRatingStarsReducer.class);

		FileInputFormat.setInputPaths(job, new Path(inputLocation));
		FileOutputFormat.setOutputPath(job, new Path(outputLocation));

		job.setJarByClass(SortRatingStarsByAvg.class);
		job.waitForCompletion(true);
	}	
	
}
