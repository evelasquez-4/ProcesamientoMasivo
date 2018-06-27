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
import org.mdp.hadoop.cli.RatingMapReduce.*;
import org.mdp.hadoop.cli.SortRatingStarsByAvg.DescendingDoubleWritable;
import org.mdp.hadoop.cli.SortRatingStarsByAvg.SortRatingStarsMapper;
import org.mdp.hadoop.cli.SortRatingStarsByAvg.SortRatingStarsReducer;
import org.mdp.hadoop.cli.StarsMapReduce.*;



public class StarsRatingJoin {
	
	public static String SPLIT_REGEX = "\\t";
	
	public static class StarsRatingJoinMap extends Mapper<Object, Text, Text, Text>{
		
		@Override
		public void map(Object key, Text value,Context output)
						throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] rawWords = line.split(SPLIT_REGEX);
			
			//ArrayList<String> tabla = new ArrayList<String>(Arrays.asList("key","value"));
	
			output.write(new Text(rawWords[0]), new Text(rawWords[1].toString()));
			
		}
	}
	
	public static class StarsRatingJoinReducer extends Reducer<Text, Text, Text, Text>
	{
		@Override
		public void reduce(Text key, Iterable<Text> values,
				Context output) throws IOException, InterruptedException {
			String val = "";int cont = 0;
			for(Text g : values)
			{	
				if(g.toString().equals("0.0")) {
					cont++;
				}
				else {
					cont++;
					val = g.toString();
				}
				//	
			}
			if(cont ==2)
				output.write(key, new Text(val));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: "+CountWords.class.getName()+" <in> <out>");
			System.exit(2);
		}
		
		String[] aux = new String[3];
		//Running UniqueCombinationMovie
		//String inputLocation = "/uhadoop/shared/imdb/imdb-stars-test.tsv";//otherArgs[0];
		String inputLocation = "/uhadoop/shared/imdb/imdb-stars.tsv";
		String outputLocation = otherArgs[1]+"tmp1";
		
		aux[0] = outputLocation;
		
		Job job = Job.getInstance(new Configuration());
	     
	    FileInputFormat.setInputPaths(job, new Path(inputLocation));
	    FileOutputFormat.setOutputPath(job, new Path(outputLocation));
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setMapperClass(StarsFilterGenderMap.class);
	    job.setReducerClass(StarsFilterGenderReduce.class);
	    
	     
	    job.setJarByClass(StarsMapReduce.class);
		job.waitForCompletion(true);
		
		//Running 
		//inputLocation = "/uhadoop/shared/imdb/imdb-ratings-test.tsv";//otherArgs[1];
		inputLocation = "/uhadoop/shared/imdb/imdb-ratings.tsv";
		outputLocation = otherArgs[1]+"tmp2";
		aux[1] = outputLocation;
		
		Job job2 = Job.getInstance(new Configuration());
	     
	    FileInputFormat.setInputPaths(job2, new Path(inputLocation));
	    FileOutputFormat.setOutputPath(job2, new Path(outputLocation));
	    
	    job2.setOutputKeyClass(Text.class);
	    job2.setOutputValueClass(Text.class);
	    job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(Text.class);
	    
	    job2.setMapperClass(RatingMapper.class);
	    job2.setReducerClass(RatingReducer.class);
	     
	    job2.setJarByClass(RatingMapReduce.class);
		job2.waitForCompletion(true);
		
		
		//Running Join
		//inputLocation = "/uhadoop/shared/imdb/imdb-stars-100k.tsv";//otherArgs[1];
		outputLocation = otherArgs[1]+"tmp3";
		aux[2] = outputLocation;
		
		Job job3 = Job.getInstance(new Configuration());
	     
	    FileInputFormat.setInputPaths(job3, new Path(aux[0]),new Path(aux[1]));
	    FileOutputFormat.setOutputPath(job3, new Path(outputLocation));
	    
	    job3.setOutputKeyClass(Text.class);
	    job3.setOutputValueClass(Text.class);
	    job3.setMapOutputKeyClass(Text.class);
	    job3.setMapOutputValueClass(Text.class);
	    
	    job3.setMapperClass(StarsRatingJoinMap.class);
	    job3.setReducerClass(StarsRatingJoinReducer.class);
	     
	    job3.setJarByClass(StarsRatingJoin.class);
	    job3.waitForCompletion(true);
		
	    
	    outputLocation = otherArgs[1];
	    
	    Job job4 = Job.getInstance(new Configuration());
	    
	    job4.setMapOutputKeyClass(DescendingDoubleWritable.class);
		job4.setMapOutputValueClass(Text.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(DoubleWritable.class);
		
		job4.setMapperClass(SortRatingStarsMapper.class); // no combiner this time!
		job4.setReducerClass(SortRatingStarsReducer.class);

	    FileInputFormat.setInputPaths(job4, new Path(aux[2]));
	    FileOutputFormat.setOutputPath(job4, new Path(outputLocation));

		job4.setJarByClass(SortRatingStarsByAvg.class);
		job4.waitForCompletion(true);
		
	}
}
