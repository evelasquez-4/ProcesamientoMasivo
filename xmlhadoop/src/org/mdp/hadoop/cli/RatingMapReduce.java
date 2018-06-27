package org.mdp.hadoop.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

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


public class RatingMapReduce {

	public static String SPLIT_REGEX = "\\t";
	public static String MOVIE_TYPE = "THEATRICAL_MOVIE";
	

	
	public static class RatingMapper extends Mapper<Object, Text, Text, Text>{
		
		private Text word = new Text();
		
		
		@Override
		public void map(Object key, Text value,Context output)
						throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] rawWords = line.split(SPLIT_REGEX);
			
			ArrayList<String> tableFields = new ArrayList<String>(Arrays.asList("VotesDistribution","VoteNumbers","AverageVote","MovieName","Year","Disambiguator","MovieType","EpisodeName"));
			
			
			String index= "";
			//DoubleWritable avg = new DoubleWritable();
			
			if(rawWords[tableFields.indexOf("MovieType")].equals(MOVIE_TYPE)  && 
					Double.parseDouble( rawWords[tableFields.indexOf("AverageVote")] ) > 7f  &&
					Integer.parseInt(rawWords[tableFields.indexOf("VoteNumbers")]) > 1000 
					
							){
				index = rawWords[tableFields.indexOf("MovieName")]+"#"+rawWords[tableFields.indexOf("Year")]+"#"+rawWords[tableFields.indexOf("Disambiguator")];
				
				//if(!rawWords[tableFields.indexOf("Year")].isEmpty())	
				word.set(index);
				//avg.set(Double.parseDouble(rawWords[tableFields.indexOf("AverageVote")]));
				output.write(word, new Text(rawWords[tableFields.indexOf("AverageVote")]));
			} 
			
		}
		
	}
	
	public static class RatingReducer extends Reducer<Text, Text, Text, Text> 
	{
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			for (Text v : values)
			{
				output.write(key, v);
			}
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
	    job.setOutputValueClass(Text.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setMapperClass(RatingMapper.class);
	    //job.setCombinerClass(MoviesReducer.class); // in this case a combiner is possible!
	    job.setReducerClass(RatingReducer.class);
	     
	    job.setJarByClass(RatingMapReduce.class);
		job.waitForCompletion(true);
	}
	
}
	