package org.mdp.hadoop.cli;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

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
import org.mdp.hadoop.cli.XmlFileSortTags.*;
import org.w3c.dom.*;
import org.w3c.dom.Document;



public class XmlFileMapReduce
{
	
	
	public static class XmlFileMap extends Mapper<Object, Text, Text, IntWritable>
	{
		private final IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		@SuppressWarnings("deprecation")
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{          
			try {
				
				InputStream is = new ByteArrayInputStream(value.toString().getBytes());
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();   
                dbFactory.setNamespaceAware(true);
            		
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(is);
				
				doc.getDocumentElement().normalize();
				
				NodeList nList = doc.getElementsByTagName("row");
				
				for(int temp = 0;temp<nList.getLength();temp++)
				{
					Node nNode = nList.item(temp);

					String tags = "";
					
					if(new Integer(nNode.getAttributes().getNamedItem("PostTypeId").getNodeValue()).intValue() == 1 )
					{
						
						 tags = ( nNode.getAttributes().getNamedItem("Tags").getNodeValue() ).equals(null) ? 
								"null":nNode.getAttributes().getNamedItem("Tags").getNodeValue();
						 
						 String [] splitTags = tags.replaceAll("<", "").replaceAll(">", ";").trim().split(";");
						 
						 for(String s : splitTags)
						 { 
							 if(s.length() >= 1)
								 word.set(s.trim());
							 
							 
							 context.write(word,one);
						 }
						 
					}
				}   
				
			} catch (Exception e) {
				System.out.println(e);
			}
			
         }  
	}
	
	public static class XmlFileReduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
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
		
		try {
			Configuration conf = new Configuration();
			
			String[] arg = new GenericOptionsParser(conf,args).getRemainingArgs();
			
			
		    String inputLocation = arg[0];
			String outputLocation = arg[1]+"tmp1";
			
			String salida_1 = outputLocation;
					
		    Job job = Job.getInstance(new Configuration());
		    
		    FileInputFormat.setInputPaths(job, new Path(inputLocation));
		    FileOutputFormat.setOutputPath(job, new Path(outputLocation));
					    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		    //job.setMapOutputValueClass(NullWritable.class);
		    
		    job.setMapOutputKeyClass(Text.class);	
		    job.setMapOutputValueClass(IntWritable.class);
		    //job.setMapOutputValueClass(NullWritable.class);
		    
			job.setMapperClass(XmlFileMap.class);
			job.setCombinerClass(XmlFileReduce.class);
			job.setReducerClass(XmlFileReduce.class);
			
//			job.setNumReduceTasks(0);
			job.setInputFormatClass(XmlInputFormat.class);
		   
		    job.setJarByClass(XmlFileMapReduce.class);	
		    job.waitForCompletion(true);
		    
		    
		    //job to sort results
		    
		    
		    Job sortJob = Job.getInstance(new Configuration());
		    
		    sortJob.setMapOutputKeyClass(DescendingIntWritable.class);
		    sortJob.setMapOutputValueClass(Text.class);
			sortJob.setOutputKeyClass(Text.class);
			sortJob.setOutputValueClass(IntWritable.class);
		    
			sortJob.setMapperClass(SortTagsCountsMapper.class); // no combiner this time!
			sortJob.setReducerClass(SortTagsCountsReducer.class);

			FileInputFormat.setInputPaths(sortJob, new Path(salida_1));
			FileOutputFormat.setOutputPath(sortJob, new Path(arg[1]));

			sortJob.setJarByClass(XmlFileMapReduce.class);
			sortJob.waitForCompletion(true);
		    
		    
		}catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}
	}
}