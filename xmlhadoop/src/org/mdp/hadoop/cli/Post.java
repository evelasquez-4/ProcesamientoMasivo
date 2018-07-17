package org.mdp.hadoop.cli;

import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Post {
	int id;
	String tags;
	

	public String getTags(){return tags;}
	public int getId(){return id;}
}
