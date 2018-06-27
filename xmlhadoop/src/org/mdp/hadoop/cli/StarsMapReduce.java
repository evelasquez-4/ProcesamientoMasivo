package org.mdp.hadoop.cli;

import java.awt.List;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class StarsMapReduce {
	public static String SPLIT_REGEX = "\\t";
	public static String MOVIE_TYPE = "THEATRICAL_MOVIE";
	public static String GENDER = "MALE";
	
	public static class StarsFilterGenderMap extends Mapper<Object, Text, Text, Text>{
		private Text word = new Text();
		@Override
		public void map(Object key, Text value,Context output)
						throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] rawWords = line.split(SPLIT_REGEX);
			
			ArrayList<String> tableFields = new ArrayList<String>(Arrays.asList("StartName","MovieName","Year","MovieNumber","MovieType","Episode","StarringAs","Role","Gender"));
			
			String index = "";
			
			if(rawWords[tableFields.indexOf("MovieType")].equals(MOVIE_TYPE))
			{
				
				index = rawWords[tableFields.indexOf("MovieName")]+"#"+rawWords[tableFields.indexOf("Year")]+"#"+rawWords[tableFields.indexOf("MovieNumber")];
			
				word.set(index);
				output.write(word, new Text(rawWords[tableFields.indexOf("Gender")]));			
			}
		}
	}
	
	public static class StarsFilterGenderReduce extends Reducer<Text,Text, Text, Text>
	{
		
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			
			boolean cond = false;
			
			for(Text g : values)
			{	
				if(!g.toString().toUpperCase().equals(GENDER))
				{
					cond = true;
					break;
				}
				else cond = false;
				
			}
			if(!cond)
				output.write(key, new Text("0.0"));
			
		}
	}
	
	/*
	
	public static class StarsMap extends Mapper<Object, Text, Text, Text>{
		private Text word = new Text();
		
		@Override
		public void map(Object key, Text value,Context output)
						throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] rawWords = line.split(SPLIT_REGEX);
			
			ArrayList<String> tableFields = new ArrayList<String>(Arrays.asList("StartName","MovieName","Year","MovieNumber","MovieType","Episode","StarringAs","Role","Gender"));
			
			String index = "";
			
			if(rawWords[tableFields.indexOf("MovieType")].equals(MOVIE_TYPE) &&
				rawWords[tableFields.indexOf("Gender")].equals(GENDER)) {
				
				index = rawWords[tableFields.indexOf("MovieName")]+"#"+rawWords[tableFields.indexOf("Year")]+"#"+rawWords[tableFields.indexOf("MovieNumber")];
			
				word.set(index.toLowerCase());
				output.write(word, new Text(""));			}
		}
	}
	
	public static class StarsReduce extends Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			
			//for(Text g : values)
			//{	
				output.write(key,new Text("a"));
			//}
		}
	}
	*/
	
	
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
	    job.setOutputValueClass(Text.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	   // job.setMapperClass(StarsMap.class);
	    //job.setCombinerClass(StarsReduce.class); // in this case a combiner is possible!
	    //job.setReducerClass(StarsReduce.class);
	     
	    job.setJarByClass(StarsMapReduce.class);
		job.waitForCompletion(true);
	}
	
}
