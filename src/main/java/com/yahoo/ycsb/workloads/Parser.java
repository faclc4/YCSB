package com.yahoo.ycsb.workloads;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.yahoo.db.DBHandler;
import com.yahoo.db.Dump;
import com.yahoo.db.Operation;
import com.yahoo.db.Page;
import com.yahoo.db.Page_id_AcessLog;
import com.yahoo.db.Replay;
import com.yahoo.db.Replay_ts_pageID;
import info.bliki.wiki.dump.IArticleFilter;
import info.bliki.wiki.dump.Siteinfo;
import info.bliki.wiki.dump.WikiArticle;
import info.bliki.wiki.dump.WikiXMLParser;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xml.sax.SAXException;

/**
 *
 * @author Fábio Coelho
 */
public class Parser implements IArticleFilter, Serializable{
    
    //DB Handlers
    DBHandler db_handler;
    SimpleDateFormat formatter_step1;
    SimpleDateFormat formatter_step2;
    EntityStore replay_ts_pageID = null;
    EntityStore pageID_AcessLog = null;
    EntityStore dump = null;
    EntityStore replay = null;
    
    PrimaryIndex replay_ts_pageID_handler = null;
    PrimaryIndex pageID_AcessLog_handler = null;
    PrimaryIndex dump_handler = null;
    PrimaryIndex replay_handler = null;
    
    //Auxiliary counters.
    int total_parser_logs =0;
    int total_read_latest =0;
    int total_read_range =0;
    int total_read_previous =0;
    
    private final String stats_file="Stats.txt";
    
    public Parser(DBHandler db_handler){
        this.db_handler=db_handler;
        //formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:SS'Z'");
        formatter_step1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssSSS");
        formatter_step2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        loadStatsFile(stats_file);
    }
    
    public final void loadStatsFile(String file_path){        
        FileInputStream input_file_stream = null;
        BufferedReader input_reader = null;
        try {
            input_file_stream = new FileInputStream(file_path);
            input_reader = new BufferedReader(new InputStreamReader(input_file_stream));
            
            String line ="";
            while((line = input_reader.readLine()) != null){
                String [] aux = line.split("\\s");
                if(aux[0].equals("#total_parsed_logs:")){
                    this.total_parser_logs=Integer.parseInt(aux[1]);
                }   
                if(aux[0].equals("#total_read_latest:")){
                    this.total_read_latest=Integer.parseInt(aux[1]);
                }    
                if(aux[0].equals("#total_read_range:")){
                    this.total_read_range=Integer.parseInt(aux[1]);
                }        
                if(aux[0].equals("#total_read_previous_version:")){
                    this.total_read_previous=Integer.parseInt(aux[1]);
                }        
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            try {
                input_reader.close();
                input_file_stream.close();
            } catch (IOException ex) {
                Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
            }  
        }
    }
    
    public final void writeStatsFile(String file_path){
        FileOutputStream output_file_stream = null;
        BufferedOutputStream output = null;
        try {           
            output_file_stream = new FileOutputStream(file_path);
            output = new BufferedOutputStream(output_file_stream);
            //write stats to file
            output.write(("#total_parsed_logs: "+total_parser_logs+"\n").getBytes());
            output.write(("#total_read_latest: "+total_read_latest+"\n").getBytes());
            output.write(("#total_read_range: "+total_read_range+"\n").getBytes());
            output.write(("#total_read_previous_version: "+total_read_previous+"\n").getBytes());
            //percentages:
            Double rlatest = (Double.parseDouble(""+total_read_latest) / Double.parseDouble(""+total_parser_logs))*100;
            Double rrange = (Double.parseDouble(""+total_read_range) / Double.parseDouble(""+total_parser_logs))*100;
            Double rprevious = (Double.parseDouble(""+total_read_previous) / Double.parseDouble(""+total_parser_logs))*100;
            
            DecimalFormat decimal_format = new DecimalFormat("#.##");
            
            output.write(("%read_latest: "+decimal_format.format(rlatest)+" %"+"\n").getBytes());
            output.write(("%read_range: "+decimal_format.format(rrange)+" %"+"\n").getBytes());
            output.write(("%read_previous_versions: "+decimal_format.format(rprevious)+" %"+"\n").getBytes());
            output.flush();
            output.close();
            output_file_stream.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                output_file_stream.close();
                output.flush();
                output.close();
                output_file_stream.close();
            } catch (IOException ex) {
                Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    
    public void step1(){
        //String file_path="/home/fabio/Documents/Replayer/Script_Environments/ReplayLogs/out-final";
        //System.out.print("Reading file: "+file_path);
        //FileInputStream input_file_stream = null;
        BufferedReader input_reader = null;
        
        try {
        //input_file_stream = new FileInputStream(file_path);    
        //input_reader = new BufferedReader(new InputStreamReader(input_file_stream));
        input_reader = new BufferedReader(new InputStreamReader(System.in));
        //Database Handlers
        replay_ts_pageID = db_handler.getReplay_ts_pageID();
        pageID_AcessLog = db_handler.getpageId_Acess_log();
        
        replay_ts_pageID_handler = replay_ts_pageID.getPrimaryIndex(Long.class,Replay_ts_pageID.class);
        pageID_AcessLog_handler = pageID_AcessLog.getPrimaryIndex(String.class, Page_id_AcessLog.class);

        String line = "";
            while ((line = input_reader.readLine()) != null) {
                long start = System.currentTimeMillis();
                boolean insert=false;
                String url = null;
                String [] aux = line.split("\\s");
                String param = aux[1].replace(".","");
                
                Long ts = formatter_step1.parse(param).getTime();               
                
                if(aux[2].startsWith("http://en") && aux[2].matches(".*org/wiki/.*")){
                    url = aux[2].replaceFirst(".*org/wiki/", "");
                    //insert into DB:
                    long get1 = System.currentTimeMillis();
                    Replay_ts_pageID record = (Replay_ts_pageID)replay_ts_pageID_handler.get(ts);
                    long get2 = System.currentTimeMillis();
                    System.out.println("step1: get1 "+(get2-get1));
                    if(record != null){
                        Map<String,Integer> map = record.getMap();
                        map.put(url, Operation.READ_LATEST);
                        long put1 = System.currentTimeMillis();
                        replay_ts_pageID_handler.putNoReturn(record);
                        long put2 = System.currentTimeMillis();
                        System.out.println("ste1: get2 "+(put2-put1));
                    }
                    else{
                        replay_ts_pageID_handler.putNoReturn(new Replay_ts_pageID(ts,url,Operation.READ_LATEST));
                    }
                    insert=true;
                    this.total_read_latest++;
                    
                } else if(aux[2].startsWith("http://en") && aux[2].matches(".*org/w/.*")){
                    url = aux[2].replaceFirst(".*org/w/index\\.php\\?", "");
                    String stringsplit[] = url.split("\\&");

                    String url_final="";
                    
                    for(String item : stringsplit){
                        if(item.startsWith("title=")){
                            if(item.replace("title=", "")!= null)
                                url_final= item.replace("title=","");
                        }
                        if(item.startsWith("oldid=")){
                            if(item.replace("oldid=", "")!= null){
                                //revId = Long.parseLong(item.replace("oldid=", ""));
                                long get3 = System.currentTimeMillis();
                                Replay_ts_pageID record = (Replay_ts_pageID)replay_ts_pageID_handler.get(ts);
                                long get4 = System.currentTimeMillis();
                                System.out.println("ste1: get2 "+(get4-get3));
                                if(record != null){
                                    Map<String,Integer> map = record.getMap();
                                    map.put(url_final, Operation.READ_PREVIOUS);
                                    long put5 = System.currentTimeMillis();
                                    replay_ts_pageID_handler.putNoReturn(record);
                                    long put6 = System.currentTimeMillis();
                                    System.out.println("ste1: put3 "+(put6-put5));
                                }
                                else{
                                    long put7 = System.currentTimeMillis();
                                    replay_ts_pageID_handler.putNoReturn(new Replay_ts_pageID(ts,url_final,Operation.READ_LATEST));
                                    long put8 = System.currentTimeMillis();
                                    System.out.println("ste1: put4 "+(put8-put7));
                                }
                                insert=true;
                                this.total_read_previous++;
                            }
                        }
                        if(item.startsWith("action=history")){
                            //history=true;
                            Replay_ts_pageID record = (Replay_ts_pageID)replay_ts_pageID_handler.get(ts);
                                if(record != null){
                                    Map<String,Integer> map = record.getMap();
                                    map.put(url_final, Operation.READ_RANGE);
                                    replay_ts_pageID_handler.putNoReturn(record);
                                }
                                else{
                                    replay_ts_pageID_handler.putNoReturn(new Replay_ts_pageID(ts,url_final,Operation.READ_RANGE));
                                }
                                insert=true;
                                this.total_read_range++;
                        }
                    }
                    url=url_final;
                }
                if(insert){
                    long get5 = System.currentTimeMillis();
                    //Check for the first and last Ts for page info.
                    Page_id_AcessLog record = (Page_id_AcessLog)pageID_AcessLog_handler.get(url);
                    Page_id_AcessLog recordX = (Page_id_AcessLog)pageID_AcessLog_handler.get(null, url, LockMode.READ_UNCOMMITTED);
                    long get6 = System.currentTimeMillis();
                    System.out.println("ste1: get3 "+(get6-get5));
                    if(record!=null){
                        if(record.getfirst() == null || record.getLast()==null){
                            record.setFirst(ts);
                            record.setLast(ts);
                        }
                        if(ts < record.getfirst()){
                            record.setFirst(ts);
                        }
                        if(ts > record.getLast()){
                            record.setLast(ts);
                        }
                        
                        long put9 = System.currentTimeMillis();
                        pageID_AcessLog_handler.putNoReturn(record);
                        long put10 = System.currentTimeMillis();
                        System.out.println("ste1: put5 "+(put10-put9));
                    }
                    else{
                        long put11 = System.currentTimeMillis();
                        pageID_AcessLog_handler.put(new Page_id_AcessLog(url,ts,ts));
                        long put12 = System.currentTimeMillis();
                        System.out.println("ste1: put6 "+(put12-put11));
                    }
                    this.total_parser_logs++;
                }
            long finale = System.currentTimeMillis();
            System.out.println("ste1: total "+(finale-start)); 
            }
            
        } catch (IOException ex) {
            throw new RuntimeException("Unable to read replay log");
        } catch (DatabaseException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ParseException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        }
        finally{
            writeStatsFile(stats_file);
            System.out.println("done");    
        }
    }
    
    public void step2(){
        dump = db_handler.getDump();
        pageID_AcessLog = db_handler.getpageId_Acess_log();
        try {
            dump_handler = dump.getPrimaryIndex(String.class, Dump.class);
            pageID_AcessLog_handler = pageID_AcessLog.getPrimaryIndex(String.class, Page_id_AcessLog.class);
            
            WikiXMLParser wxp = new WikiXMLParser(new BufferedInputStream(System.in), this);    
            wxp.parse();
                    
        } catch (IOException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        } catch (DatabaseException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        } catch (SAXException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
     @Override
    public void process(WikiArticle page, Siteinfo stnf) throws SAXException {
        try {          
            String url = page.getTitle();
            final Long ts = formatter_step2.parse(page.getTimeStamp()).getTime();
            final String RevId = page.getRevisionId();
            final Long size = Long.parseLong(""+page.getText().length());
           
            //checkes wheather the page exists in table [pageId -> firstAccess, Last Acess]
            Page_id_AcessLog article = (Page_id_AcessLog) pageID_AcessLog_handler.get(url);
            
            if(article!=null){
                if(ts < article.getLast()){
                    Map<Long,Long> record = ((Dump)dump_handler.get(url)).getMap();
                    record.put(ts, size);
                    dump_handler.putNoReturn(new Dump(url,record));
                }
            }
            else{
                dump_handler.putNoReturn(new Dump(url,ts,size));
            }
        } catch (ParseException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        } catch (DatabaseException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        } catch (NullPointerException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public void step3(){
        try {
            replay_ts_pageID = db_handler.getReplay_ts_pageID();
            pageID_AcessLog = db_handler.getpageId_Acess_log();
            dump = db_handler.getDump();
            replay = db_handler.getReplay();
            
            replay_ts_pageID_handler = replay_ts_pageID.getPrimaryIndex(Long.class,Replay_ts_pageID.class);
            pageID_AcessLog_handler = pageID_AcessLog.getPrimaryIndex(String.class, Page_id_AcessLog.class);
            dump_handler = dump.getPrimaryIndex(String.class, Dump.class);
            replay_handler = replay.getPrimaryIndex(Long.class, Replay.class);
            
            EntityCursor<Replay_ts_pageID> cursor1 = replay_ts_pageID_handler.entities();
            
            Replay_ts_pageID record = null;
            
            while((record = cursor1.next())!= null){
                Long ts = record.getTs();
                List<Page> pages = new ArrayList<Page>();
                Long revId = null;
                
                for(Map.Entry<String,Integer> page : record.getMap().entrySet()){
                    if(page.getValue()==Operation.READ_LATEST || page.getValue()==Operation.READ_RANGE){
                        if(dump_handler.contains(page.getKey())){
                            //revId = ((Dump)dump_handler.get(page.getKey())).getLastMapIndex();
                            revId = ((Dump)dump_handler.get(page.getKey())).getLastMapIndex();
                            pages.add(new Page(page.getKey(),page.getValue(),revId));
                        }
                        else break;
                    }
                    if(page.getValue()==Operation.READ_PREVIOUS){
                        if(dump_handler.contains(page.getKey())){
                            revId = ((Dump)dump_handler.get(page.getKey())).getFirstMapIndex();
                            pages.add(new Page(page.getKey(),page.getValue(),revId));
                        }
                        else break;
                    }
                }
                replay_handler.putNoReturn(new Replay(ts,pages));
            }
        } catch (DatabaseException ex) {
            Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
     public static void main(String[]args){  
        if(args.length==2){
            try {
                String db_path = args[1];
                
                DBHandler bd_handler = new DBHandler(db_path);
                //bd_handler.init();
                
                
                Parser parser = new Parser(bd_handler);
                
                if(args[0].equals("step1")){
                   parser.step1();
                }
                if(args[0].equals("step2")){
                    parser.step2();
                }
                if(args[0].equals("step3")){
                    parser.step3();
                }
                
                
                bd_handler.closeConn(); 
           } catch (Exception e) {
                e.printStackTrace();
            }
        } 
        else{             
            System.err.println("Missing parameters");
            System.err.println("(step1|step2|step3) database_path");
            System.exit(1);
        }
    }
}
