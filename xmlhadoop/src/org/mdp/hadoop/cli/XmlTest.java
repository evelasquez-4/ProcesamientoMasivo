package org.mdp.hadoop.cli;

import java.io.ByteArrayInputStream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.util.zip.InflaterInputStream;

import javax.swing.text.ElementIterator;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.events.XMLEvent;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;




public class XmlTest {
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws XMLStreamException,IOException {
		
		try {

//			File fXmlFile = new File("/Users/raven/Desktop/Posts.xml");
			File fXmlFile = new File("/Users/raven/Documents/Posts.xml");
	
	
			
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(fXmlFile);
		
            doc.getDocumentElement().normalize();
			NodeList nList = doc.getElementsByTagName("row");
						
			int preg_tipo1 = 0;
			int preg_tipo2 = 0;
			int preg_tipo3 = 0;
			int preg_tipo4 = 0;
			
				for (int temp = 0; temp < nList.getLength(); temp++) {
	
					Node nNode = nList.item(temp);
					
					
					
					if( new Integer(nNode.getAttributes().getNamedItem("PostTypeId").getNodeValue()).intValue() == 1)
					{
						String cad = nNode.getAttributes().getNamedItem("Body").getNodeValue();	
						
						//post tiene codigo fuente en la descripcion
						if(StringUtils.contains(cad,"<code>") && StringUtils.contains(cad, "</code>"))
						{
							String code = StringUtils.substringBetween(cad,"<code>","</code>");
							//verificacion existencia de de respuesta, opciones tiene y no tiene respuesta
							try
							{
								if(!nNode.getAttributes().getNamedItem("AcceptedAnswerId").getNodeValue().equals(NullPointerException.class)) {
//									System.out.println("1");
									preg_tipo1 ++;
									
								}
							}catch(NullPointerException e){

//								System.out.println("2");
								preg_tipo2++;
							
							}
							
							
						}
						else
						{

						}
									
					}

				}
				System.out.println(preg_tipo1);
				System.out.println(preg_tipo2);
			
			
			
		} catch (Exception e) {
			// TODO: handle exception
			System.err.println(e);
		}
	}

}
