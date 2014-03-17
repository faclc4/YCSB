/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.yahoo.ycsb.workloads;

import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.xml.sax.SAXException;


/*
 * Adapted from demo application of bliki project.
 * 
 * Reads a compressed or uncompressed Wikipedia XML dump
 * file (depending on the given file extension <i>.gz</i>, <i>.bz2</i> or
 * <i>.xml</i>) and prints the title and wiki text.
 * 
 * @author Valerio Schiavoni
 */

public class CompressedDumpParser implements IArticleFilter, Serializable{

	    private static final long serialVersionUID = 1L;
		public int pages = 0;
		public int revisions = 0;
		public HashMap<BigInteger, List<Long>> pageRevisions = new HashMap<BigInteger, List<Long>>();

		//2011-07-17T19:30:32Z
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		
		
		@SuppressWarnings("serial")
		public void process(final WikiArticle page, Siteinfo siteinfo)
				throws SAXException {
			
			revisions++;
			
			BigInteger pId = new BigInteger(page.getId());
			try {
				final long ts = formatter.parse(page.getTimeStamp()).getTime();
				if (pageRevisions.containsKey(pId)) {
					pageRevisions.get(pId).add(ts);
				} else {
						pageRevisions.put(pId, new ArrayList<Long>(){{new Long(ts);}});
						pages++;
				}
				
				
			} catch (ParseException e) {
				throw new SAXException("Error parsing date: " + page.getTimeStamp(), e);
			}
			
			if ((pages % 1000) == 0) {
				System.out.print("Overall pages read so far: "+ pages + " Revisions: " + revisions  + "\r");
			}
			
		}
	
	public HashMap<BigInteger, List<Long>> readData( InputStream stream ) throws Exception {
	    FSTObjectInput in = new FSTObjectInput(stream);
	    @SuppressWarnings("unchecked")
		HashMap<BigInteger, List<Long>> result = (HashMap<BigInteger, List<Long>>)in.readObject();
	    in.close(); // required !
	    return result;
	}

	public void writeData( OutputStream stream, HashMap<BigInteger, List<Long>> data) throws Exception {
	    FSTObjectOutput out = new FSTObjectOutput(stream);
	    out.writeObject( data );
	    out.close(); 
	}

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		String bz2Filename = null; 
		if (args.length == 0) {
			System.out.println("Reading from stdin");
		} else {
			bz2Filename = args[0];
			System.out.println("Reading from file: " + bz2Filename);
		}
		
		try {
			CompressedDumpParser  handler = new CompressedDumpParser();
			WikiXMLParser wxp = null;
			if(bz2Filename != null) {
				wxp = new WikiXMLParser(bz2Filename, handler);
			} else {
				wxp = new WikiXMLParser(new BufferedInputStream(System.in), handler);
			}
			wxp.parse();
			
			
			System.out.println( handler.pageRevisions.keySet().size() + " pages read. Revisions:" + handler.revisions);

			System.out.println("Dumping data");
			handler.writeData(new FileOutputStream("dump.obj"), handler.pageRevisions);
			
			System.out.println("That's all folks!");
			
			//reading test
			//HashMap<BigInteger, List<Long>> data = handler.readData(new FileInputStream("dump.obj"));
			//System.out.println("Read pages:" + data.keySet().size());
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}

		

	
	}
}