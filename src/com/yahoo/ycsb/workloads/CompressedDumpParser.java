package com.yahoo.ycsb.workloads;

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

import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
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
		public HashMap<String, List<Long>> pageRevisions = new HashMap<String, List<Long>>();
                
                public static HashMap<String,Integer> stats = new HashMap<String,Integer>();
                
                public static Map<String,Integer> stats_clean = new TreeMap<String,Integer>();
                
                public HashMap<String,Long> revisionIds = new HashMap<String,Long>();

		//2011-07-17T19:30:32Z
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
                
                SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM");
		
		
		@SuppressWarnings("serial")
		public void process(final WikiArticle page, Siteinfo siteinfo)
				throws SAXException {
			
			revisions++;
			
			BigInteger pId = new BigInteger(page.getId());
                        String url = page.getTitle();
                        
			try {
				final long ts = formatter.parse(page.getTimeStamp()).getTime();
                                final String RevId = page.getRevisionId();
				if (pageRevisions.containsKey(url)) {
					pageRevisions.get(url).add(ts);
				} else {
						pageRevisions.put(url, new ArrayList<Long>());
                                                pageRevisions.get(url).add(ts);
						pages++;
				}
                                
                                if(!revisionIds.containsKey(RevId)){
                                    revisionIds.put(RevId, ts);
                                }
				
				
			} catch (ParseException e) {
				throw new SAXException("Error parsing date: " + page.getTimeStamp(), e);
			}
			
			if ((pages % 1000) == 0) {
				System.out.print("Overall pages read so far: "+ pages + " Revisions: " + revisions  + "\r");
			}
			
		}
	
	public HashMap<String, List<Long>> readData( InputStream stream ) throws Exception {
	    FSTObjectInput in = new FSTObjectInput(stream);
	    @SuppressWarnings("unchecked")
		HashMap<String, List<Long>> result = (HashMap<String, List<Long>>)in.readObject();
	    in.close(); // required !
	    return result;
	}
        
        public HashMap<String, Long> readOldID( InputStream stream ) throws Exception {
	    FSTObjectInput in = new FSTObjectInput(stream);
	    @SuppressWarnings("unchecked")
		HashMap<String,Long> result = (HashMap<String, Long>)in.readObject();
	    in.close(); // required !
	    return result;
	}

	public void writeData( OutputStream stream, HashMap<String, List<Long>> data) throws Exception {
	    FSTObjectOutput out = new FSTObjectOutput(stream);
	    out.writeObject( data );
	    out.close(); 
	}
        
        public void writeDataRevs( OutputStream stream, HashMap<String,Long> data) throws Exception {
	    FSTObjectOutput out = new FSTObjectOutput(stream);
	    out.writeObject( data );
	    out.close(); 
	}
        
        public void writeStats(String filename, Map<String,Integer> data) throws FileNotFoundException{
            PrintWriter out = null;
                try {
                    out = new PrintWriter(new BufferedWriter(new FileWriter(filename, true)));
                    for(String key : data.keySet()){
                        String output = key +'\t'+data.get(key);
                        out.println(output);
                    }       out.flush();
                } catch (IOException ex) {
                    Logger.getLogger(CompressedDumpParser.class.getName()).log(Level.SEVERE, null, ex);
                } finally {
                    out.close();
                }
        }
        
        public void perDayStats(String file_path){
            System.out.println("Outputing file : "+file_path);
            FileInputStream input_file_stream = null;
            try {
               input_file_stream = new FileInputStream(file_path);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(CompressedDumpParser.class.getName()).log(Level.SEVERE, null, ex);
            }

                BufferedReader input_reader = new BufferedReader(new InputStreamReader(input_file_stream));

                String line = "";
            try {            
                while ((line = input_reader.readLine()) != null) {
                    String [] aux = line.split("\\s");
                    String param = aux[1].replace(".","");
                    Long ts = Long.parseLong(param);
                    
                    Calendar time = Calendar.getInstance();
                    time.setTimeInMillis(ts);
                    int day = time.get(Calendar.DAY_OF_MONTH);
                    int month = time.get(Calendar.MONTH);
                    int year = time.get(Calendar.YEAR);
                    
                    String key = ""+year+"-"+month+"-"+day;
                                
                    if(!stats.containsKey(key)){
                       stats.put(key, 1);
                    }
                    else{
                        int cur = stats.get(key);
                        stats.put(key, cur+1);
                    }        
                }
                

            } catch (IOException ex) {
                Logger.getLogger(CompressedDumpParser.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        public void correctStats(String file_path) throws ParseException{
            System.out.println("Correcting file : "+file_path);
            FileInputStream input_file_stream = null;
            try {
               input_file_stream = new FileInputStream(file_path);
            } catch (FileNotFoundException ex) {
                Logger.getLogger(CompressedDumpParser.class.getName()).log(Level.SEVERE, null, ex);
            }

                BufferedReader input_reader = new BufferedReader(new InputStreamReader(input_file_stream));

                String line = "";
            try {            
                while ((line = input_reader.readLine()) != null) {
                    String [] aux = line.split("\\s");
                    
                    String key = aux[0];
                    int value = Integer.parseInt(aux[1]);
                    
                    if(!stats_clean.containsKey(key)){
                        Date date = new SimpleDateFormat("yyyy-M-dd", Locale.ENGLISH).parse(key);
                        stats_clean.put(key, value);
                    }
                    else{
                        int val_aux = stats_clean.get(key);
                        stats_clean.put(key, value+val_aux);
                    }
                }
                

            } catch (IOException ex) {
                Logger.getLogger(CompressedDumpParser.class.getName()).log(Level.SEVERE, null, ex);
            }
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
			//handler.writeData(new FileOutputStream("dump.obj"), handler.pageRevisions);
                        handler.writeDataRevs(new FileOutputStream("dumpRevs.obj"), handler.revisionIds);
                        
                        //System.out.println("Dumping stats");
                        //handler.writeStats("stats.txt", stats);
			System.out.println("That's all folks!");
			
			//reading test
			//HashMap<BigInteger, List<Long>> data = handler.readData(new FileInputStream("dump.obj"));
			//System.out.println("Read pages:" + data.keySet().size());
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
            /*
            CompressedDumpParser  handler = new CompressedDumpParser();
            //handler.perDayStats(args[0]);
            handler.correctStats(args[0]);
            handler.writeStats("stats_clean.txt", stats_clean);
            //handler.writeStats("stats.txt", stats);
            System.out.println("That's all folks!");
            */
	}
}