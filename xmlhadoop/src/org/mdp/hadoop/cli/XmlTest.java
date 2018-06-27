package org.mdp.hadoop.cli;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.Reader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XmlTest {
	
	public static void main(String[] args) {
		
		try {
			
			//File fXmlFile = new File("/Users/raven/eclipse-workspace/xmlhadoop/data/data.xml");
			File fXmlFile = new File("/Users/raven/Downloads/Users.xml");
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);
			
			
			
			doc.getDocumentElement().normalize();
			
			NodeList nList = doc.getElementsByTagName("row");
			
			for (int temp = 0; temp < nList.getLength(); temp++) {

				Node nNode = nList.item(temp);
						
				System.out.println("\nCurrent Element :" + nNode.getNodeName());
				//System.out.println(nNode.getAttributes().getNamedItem("Reputation").getNodeValue());
				System.out.println(nNode.getAttributes().getNamedItem("DisplayName").getNodeValue());
						
				//if (nNode.getNodeType() == Node.ELEMENT_NODE) {

				//	Element eElement = (Element) nNode;
					
					
					
					//System.out.println("row : " + eElement.getElementsByTagName("row").item(0).getTextContent());
					
					//System.out.println("Price : " + eElement.getElementsByTagName("price").item(0).getTextContent());
					//System.out.println("Description : " + eElement.getElementsByTagName("description").item(0).getTextContent());
					

				//}
			}
			
			
		} catch (Exception e) {
			// TODO: handle exception
			System.err.println(e);
		}
	}

}
