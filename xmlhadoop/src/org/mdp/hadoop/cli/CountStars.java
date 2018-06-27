package org.mdp.hadoop.cli;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.mdp.hadoop.cli.CountWords.CountWordsMapper;
import org.mdp.hadoop.cli.CountWords.CountWordsReducer;

public class CountStars {
	
	public static String SPLIT_REGEX = "\\t";
	public static String MOVIE_TYPE = "THEATRICAL_MOVIE";
	
	public static class CountPairsMapper extends Mapper<Object, Text, Text, Text>{

		private final IntWritable one = new IntWritable(1);
		private Text word = new Text();

		
		/**
		 * Given the offset in bytes of a line (key) and a line (value),
		 * we will output (word,1) for each word in the line.
		 * 
		 * @throws InterruptedException 
		 * 
		 */
		 @Override
		public void map(Object key, Text value,Context output)
						throws IOException, InterruptedException {
			Text startName = new Text(); 
			String line = value.toString();
			String[] rawWords = line.split(SPLIT_REGEX);
			ArrayList<String> tabla = new ArrayList<String>(Arrays.asList("StartName","MovieName","Year","MovieNumber","MovieType","Episode","StarringAs","Role","Gender"));
			
			if(rawWords[tabla.indexOf("MovieType")].equals(MOVIE_TYPE)) {
					
					String concat = "";
					
					if(!rawWords[tabla.indexOf("MovieNumber")].isEmpty()) {
						concat =    rawWords[tabla.indexOf("MovieName")]+"##"
							       +rawWords[tabla.indexOf("Year")]+"##"
								   +rawWords[tabla.indexOf("MovieNumber")];
						
					}
					else {
						concat = rawWords[tabla.indexOf("MovieName")]+"/";
							       
					}
					word.set(concat.toLowerCase());
					startName.set(rawWords[tabla.indexOf("StartName")]);
					output.write(word,startName);
					
					
				
							
			}
		}
		 
	}
	
	
	public static class CountPairsReducer extends Reducer<Text, Text, Text, Text> {

		/**
		 * Given a key (a word) and all values (partial counts) for 
		 * that key produced by the mapper, then we will sum the counts and
		 * output (word,sum)
		 * 
		 * @throws InterruptedException 
		 */
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			
			List<String> pares = new ArrayList<String>();
			for(Text value:values) {
				pares.add(value.toString());
			}
			
			for(int i = 0 ;i< pares.size();i++) {
				for(int j = i+1; j< pares.size();j++)
				{
					output.write(new Text(pares.get(i)), new Text(pares.get(j)));
				}
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
	    
	    job.setMapperClass(CountPairsMapper.class);
	    //job.setCombinerClass(CountPairsReducer.class); // in this case a combiner is possible!
	    job.setReducerClass(CountPairsReducer.class);
	     
	    job.setJarByClass(CountStars.class);
		job.waitForCompletion(true);
	}	
}
