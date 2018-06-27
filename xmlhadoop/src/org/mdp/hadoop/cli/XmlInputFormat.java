package org.mdp.hadoop.cli;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


/**
 * Reads records that are delimited by a specific begin/end tag.
 */
public class XmlInputFormat extends TextInputFormat {

  public static final String START_TAG_KEY = "xmlinput.start";
  public static final String END_TAG_KEY = "xmlinput.end";

  

  @Override
  public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context){
    
	  return new XmlRecordReader();
      
  }
 
}
