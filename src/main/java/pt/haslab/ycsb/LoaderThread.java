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

import pt.haslab.db.Dump;
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
