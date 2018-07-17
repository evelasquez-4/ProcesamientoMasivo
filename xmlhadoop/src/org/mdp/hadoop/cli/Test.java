package org.mdp.hadoop.cli;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;

import javax.print.attribute.standard.PrinterLocation;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;


import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;




public class Test  extends DefaultHandler{
	
	 private Post acct;
     private String temp;
     private ArrayList<Post> accList = new ArrayList<Post>();


	
	public static void main(String[] args) throws ParserConfigurationException, IOException, SAXException, XMLStreamException{
		File xmlFile = new File("/Users/raven/eclipse-workspace/xmlhadoop/data/Posts.xml");
		
		StringReader sr = new StringReader("<posts> <row Id='10' PostTypeId='asdasdasdasdasdasdasda'/> </posts>");
	      XMLInputFactory factory = XMLInputFactory.newInstance();
	      XMLStreamReader parser = factory.createXMLStreamReader(sr);
	      while (parser.hasNext()){
	         int event = parser.next();
	         
	         if (event == XMLStreamConstants.START_ELEMENT){ 
		            
		            if (parser.getLocalName().equals("row")){
		            	String id = parser.getAttributeValue(null,"PostTypeId");
		            	
		            	System.out.println(id);
			         }
		            
		            
		      }
	      }

        
	}
	
	private void readList() {
        //System.out.println("No of  the accounts in bank '" + ac  + "'.");
        Iterator<Post> it = accList.iterator();
        while (it.hasNext()) {
               System.out.println(it.next().toString());
        }
 }



//	public List<String> borraCaracteres(String cadena)
//	{
//		String aux = cadena;
//			
//		if(aux.contains("&lt;")) {
//			aux = aux.replaceAll("&lt;", "");
//			aux = aux.replaceAll("&gt", "").trim();
//			
//			
//			
//			
//			
//			
//			return removeNull;
//		}
//		
//		return null;
//	}

}
