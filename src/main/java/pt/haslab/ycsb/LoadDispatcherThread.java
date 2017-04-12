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

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import pt.haslab.db.DBHandler;
import pt.haslab.db.Dump;
import pt.haslab.ycsb.measurements.ResultHandler;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Fábio Coelho
 */
public class LoadDispatcherThread implements Runnable{
       
    DB _db;
    boolean _speedup;
    Workload _workload;
    ClientThreadState _workloadstate;
    Properties _props;
    DBHandler bd_handler= null;
    EntityStore dump = null;
    PrimaryIndex dump_handler = null;
    public OpCounter opscounter=null;    
    Workload workload=null;
    ExecutorService thread_pool=null;
    
    AtomicLong bytecounter = new AtomicLong();
    
    int tcount= 0;
    
    private final String table="usertable";
    
    
    public LoadDispatcherThread(String db_path,OpCounter opcounter,DB db, Workload workload,ExecutorService thread_pool,Properties props){
        try { 
            this.bd_handler = new DBHandler(db_path);
            
            dump = bd_handler.getDump();
            dump_handler = dump.getPrimaryIndex(String.class, Dump.class);
            
            this._db=db;
            _db.init();
            
            this.opscounter=opcounter;
            this.workload=workload;
            this.thread_pool=thread_pool;
            this._props=props;
            
        } catch (DatabaseException | DBException ex) {
            Logger.getLogger(DispatcherThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    @Override
    public void run() {
        try {
            ClientThreadState state = (ClientThreadState) workload.initThread(_props,1,1);
            ResultHandler resultHandler = state.getClient_resultHandler();
                       
            EntityCursor<Dump> cursor = dump_handler.entities();

            Dump record = null;
            while((record = cursor.next())!= null && bytecounter.get() < 2000000000){
               thread_pool.execute(new LoaderThread(_db,record,opscounter,bytecounter));
            }
            
            //Stop Thread Pool
            thread_pool.shutdown();
            thread_pool.awaitTermination(1, TimeUnit.MINUTES);
        } catch (WorkloadException ex) {
            Logger.getLogger(DispatcherThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (DatabaseException ex) {
            Logger.getLogger(LoadDispatcherThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(LoadDispatcherThread.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }
    
    
    
}