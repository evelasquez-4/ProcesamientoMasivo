package org.mdp.hadoop.cli;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.w3c.dom.*;
import org.w3c.dom.Document;


public class XmlFileMapReduce
{
	
	public static class XmlFileMap extends Mapper<LongWritable, Text, Text, NullWritable>
	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{          
     
            try {
            	
            		InputStream is = new ByteArrayInputStream(value.toString().getBytes());
                DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();           	
            		
				DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
				Document doc = dBuilder.parse(is);
				
				doc.getDocumentElement().normalize();
				
				NodeList nList = doc.getElementsByTagName("row");
				
				for(int temp = 0;temp<nList.getLength();temp++)
				{
					Node nNode = nList.item(temp);
					//System.out.println("\nCurrent Element :" + nNode.getNodeName());
					
					String id = ( nNode.getAttributes().getNamedItem("DisplayName").getNodeValue() ).equals(null) ? 
							"null":nNode.getAttributes().getNamedItem("DisplayName").getNodeValue();
					String reputation = ( nNode.getAttributes().getNamedItem("Reputation").getNodeValue() ).equals(null) ?
							"null":nNode.getAttributes().getNamedItem("Reputation").getNodeValue();
					String accountId = ( nNode.getAttributes().getNamedItem("AccountId").getNodeValue() ).equals(null)?
							"null": nNode.getAttributes().getNamedItem("AccountId").getNodeValue();
					
					context.write(new Text(id+","+reputation), NullWritable.get());
					
//					if(nNode.getNodeType() == Node.ELEMENT_NODE)
//					{
//						Element eElement =  (Element) nNode;
//						
//						String name = eElement.getElementsByTagName("name").item(0).getTextContent();
//						System.out.println(name);
//						String price = eElement.getElementsByTagName("price").item(0).getTextContent();
//	                    String calories = eElement.getElementsByTagName("calories").item(0).getTextContent();
//	                     
//	                    String o = name+","+price+","+","+calories;
//	                    context.write(new Text(name), NullWritable.get());
//	                     
//					}
            	
				}
            }catch (Exception e) {
				// TODO: handle exception
            		System.out.println(e);
			}
			
         }  
	}
	
	

	public static void main(String[] args) throws Exception {
		
		try {
			Configuration conf = new Configuration();
			
			String[] arg = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		    conf.set("xmlinput.start", "<users>");
		    conf.set("xmlinput.end", "</users>");
			
			
			Job job = new Job(conf,"map reduce");			
			job.setJarByClass(XmlFileMapReduce.class);
			job.setMapperClass(XmlFileMap.class);
			
			job.setNumReduceTasks(0);
			
			job.setInputFormatClass(XmlInputFormat.class);
			
		    job.setMapOutputKeyClass(Text.class);	
		    //job.setMapOutputValueClass(LongWritable.class);
		    job.setMapOutputValueClass(NullWritable.class);
		    
		    job.setOutputKeyClass(Text.class);
		    //job.setOutputValueClass(LongWritable.class);
		    job.setMapOutputValueClass(NullWritable.class);
	
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		   
		
		    job.waitForCompletion(true);
		}catch (Exception e) {
			// TODO: handle exception
			System.out.println(e);
		}
	}
}
