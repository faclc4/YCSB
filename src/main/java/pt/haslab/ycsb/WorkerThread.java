/*
 * Copyright 2016 by INESC TEC                                               
 * Developed by Fábio Coelho                                                 
 * This work was based on the YCSB Project from Yahoo!                          
 *
 * Licensed under the Apache License, Version 2.0 (the "License");           
 * you may not use this file except in compliance with the License.          
 * You may obtain a copy of the License at                                   
 *
 * http://www.apache.org/licenses/LICENSE-2.0                              
 *
 * Unless required by applicable law or agreed to in writing, software       
 * distributed under the License is distributed on an "AS IS" BASIS,         
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
 * See the License for the specific language governing permissions and       
 * limitations under the License.                                            
 */
package pt.haslab.ycsb;

import pt.haslab.db.Operation;
import pt.haslab.db.Page;
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
