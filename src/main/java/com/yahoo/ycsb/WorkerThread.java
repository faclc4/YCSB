package com.yahoo.ycsb;

import com.yahoo.db.Operation;
import com.yahoo.db.Page;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.infinispan.versioning.utils.version.VersionScalar;

/**
 *
 * @author Fábio Coelho
 */
public class WorkerThread implements Runnable{

    DB db;
    private List<Page> pages;
    private final String table="usertable";
    long time;
    OpCounter opcounter;
    
    public WorkerThread(DB db, List<Page> pages, long time,OpCounter opcounter){
        this.db=db;
        this.pages=pages; 
        this.time = time;
        this.opcounter = opcounter;
    }
    
    @Override
    public void run() {
        try {
            for(Page pg : pages){
                String op=null;
                if(pg.getop()==Operation.READ_LATEST){
                    //SUBMIT READ LATEST OP
                    db.read(table, pg.getUrl());
                    op = "READ";
                    opcounter.incrementOp();
                    //System.out.println("[Read_Latest] : (key): "+pg.getUrl()+" ");
                    //System.out.println(pg.getUrl());
                }
                if(pg.getop()==Operation.READ_PREVIOUS){
                    //SUBMIT READ PREVIOUS OP
                    db.read(table, pg.getUrl(), new VersionScalar(pg.getRevId()));
                    op = "READ_PREVIOUS";
                    opcounter.incrementOp();
                    //System.out.println("[Read_Previous] : (key): "+pg.getUrl()+" version: "+pg.getRevId()+" ");
                    //System.out.println(pg.getUrl());
                }
                if(pg.getop()==Operation.READ_RANGE){
                    //SUBMIT READ RANGE OP
                    db.readRange(table, pg.getUrl(), new VersionScalar(0l), new VersionScalar(pg.getRevId()));
                    op="READ_RANGE";
                    opcounter.incrementOp();
                    //System.out.println("[Read_Range] : (key): "+pg.getUrl()+" version: "+pg.getRevId()+" ");
                    //System.out.println(pg.getUrl());
                }
            }
            //Thread.sleep(time);
        } catch (Exception ex) {
            Logger.getLogger(WorkerThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
}
