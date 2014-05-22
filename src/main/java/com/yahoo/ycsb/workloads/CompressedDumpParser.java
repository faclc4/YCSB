package com.yahoo.ycsb.workloads;

import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import org.xml.sax.SAXException;

import java.io.*;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
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
    public static HashMap<String,Integer> stats = new HashMap<String,Integer>();
    public static Map<String,Integer> stats_clean = new TreeMap<String,Integer>();

    public int pages = 0;
    public int revisions = 0;
    public HashMap<String, List<Long>> pageRevisions = new HashMap<String, List<Long>>();
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

    public HashMap<String, List<Long>> readDump(InputStream stream) throws Exception {
        FSTObjectInput in = new FSTObjectInput(stream);
        @SuppressWarnings("unchecked")
        HashMap<String, List<Long>> result = (HashMap<String, List<Long>>)in.readObject();
        in.close(); // required !
        return result;
    }

    public HashMap<String, Long> readRevisions(InputStream stream) throws Exception {
        FSTObjectInput in = new FSTObjectInput(stream);
        @SuppressWarnings("unchecked")
        HashMap<String,Long> result = (HashMap<String, Long>)in.readObject();
        in.close(); // required !
        return result;
    }

    public HashMap<Long,Long> readSizes(InputStream stream) throws Exception {
        FSTObjectInput in = new FSTObjectInput(stream);
        @SuppressWarnings("unchecked")
        HashMap<Long,Long> result = (HashMap<Long,Long>)in.readObject();
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
                int hour = time.get(Calendar.HOUR_OF_DAY);
                int minute = time.get(Calendar.MINUTE);
                int second = time.get(Calendar.SECOND);

                String key = ""+year+"/"+month+"/"+day+"-"+hour+":"+minute+":"+second;

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
        String outputName = "dump";
        if (args.length == 0) {
            System.out.println("Reading from stdin");
        } else {
            bz2Filename = args[0];
            outputName = bz2Filename.split("xml")[0];
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

            System.out.println("Dumping data ... ");
            handler.writeData(new FileOutputStream(outputName+"dat"), handler.pageRevisions);
            handler.writeDataRevs(new FileOutputStream(outputName+"rev"), handler.revisionIds);
            System.out.println("That's all folks!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
