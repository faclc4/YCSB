/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.yahoo.ycsb;

import com.yahoo.db.Dump;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.infinispan.versioning.utils.version.Version;
import org.infinispan.versioning.utils.version.VersionScalar;

/**
 *
 * @author Fábio Coelho <fabio.a.coelho@inesctec.pt>
 */
public class LoaderThread implements Runnable{
    DB db;
    private final String table="usertable";
    Dump record;
    OpCounter opcounter;
    HashMap<Version,ByteIterator> content;
    AtomicLong byteounter;
    
    public LoaderThread(DB db,Dump record,OpCounter opcounter,AtomicLong bytecounter){
        this.db=db;
        this.record = record;
        this.opcounter = opcounter;
        this.byteounter=bytecounter;
    }
    
    @Override
    public void run() {
        try{
            content = new HashMap<>();
            
            if(record.getMap()!=null){  
                
                loop : {
                for(Map.Entry<Long,Long> revision : record.getMap().entrySet()){
                       content.put(new VersionScalar(revision.getKey()), new RandomByteIterator(revision.getValue()));
                       //System.out.println("\t [version]: "+revision.getKey()+" [size] (bytes): "+revision.getValue());
                       this.byteounter.addAndGet(revision.getValue());
                       break loop;
                   }
                }
                int res = db.insert(table, record.getPageId(), content);
                //int res = db.read(table, record.getPageId());
                System.out.println(record+": "+res);
                opcounter.incrementOp();
            }
            else{
                content.put(new VersionScalar(0l), new RandomByteIterator(100));  
                System.out.println("\t [generated]: 0l [size] (bytes): 100");
                db.insert(table, record.getPageId(), content);
                //int res = db.read(table, record.getPageId());
                //System.out.println(record+": "+res);
                this.byteounter.addAndGet(100);
                opcounter.incrementOp();
            }
            
                        
            
        } catch (Exception ex) {
            Logger.getLogger(WorkerThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
