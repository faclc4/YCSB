/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.yahoo.ycsb;

import com.yahoo.db.Operation;
import com.yahoo.db.Page;
import com.yahoo.ycsb.measurements.ResultHandler;
import org.infinispan.versioning.utils.version.VersionScalar;

/**
 *
 * @author Fábio Coelho
 */
public class WorkerThread implements Runnable{

    DB db;
    private Page pg;
    ResultHandler resultHandler;
    private final String table="usertable";
    
    
    
    public WorkerThread(DB db, Page page,ResultHandler resultHandler){
        this.db=db;
        this.pg=page;
        this.resultHandler=resultHandler;
    }
    
    @Override
    public void run() {
        long init_transaction_time = System.currentTimeMillis();
        String op=null;
        if(pg.getop()==Operation.READ_LATEST){
            //SUBMIT READ LATEST OP                        
            db.read(table, pg.getUrl());
            op = "READ";
        }
        if(pg.getop()==Operation.READ_PREVIOUS){
            //SUBMIT READ PREVIOUS OP
            db.read(table, pg.getUrl(), new VersionScalar(pg.getRevId()));
            op = "READ_PREVIOUS";
        }
        if(pg.getop()==Operation.READ_RANGE){
            //SUBMIT READ RANGE OP                        
            db.readRange(table, pg.getUrl(), new VersionScalar(0L), new VersionScalar(pg.getRevId()));
            op="READ_RANGE";
        }
        long end_transaction_time = System.currentTimeMillis();
        //resultHandler.recordTimeline(op, init_transaction_time, end_transaction_time);
    }
    
    
}
