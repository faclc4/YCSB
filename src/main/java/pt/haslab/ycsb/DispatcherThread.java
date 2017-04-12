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
import pt.haslab.db.Replay;
import pt.haslab.ycsb.measurements.ResultHandler;
import static pt.haslab.ycsb.workloads.File_CoreWorkload.SPEEDUP_PROPERTY;
import static pt.haslab.ycsb.workloads.File_CoreWorkload.SPEEDUP_PROPERTY_DEFAULT;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Fábio Coelho
 */
public class DispatcherThread implements Runnable{
//This single thread reads the replay database and instructs workerthreads (clients) to perform actions.
    
    DB _db;
    boolean _speedup;
    Workload _workload;
    ClientThreadState _workloadstate;
    Properties _props;
    DBHandler bd_handler= null;
    EntityStore replay = null;
    PrimaryIndex replay_handler = null;
    public OpCounter opscounter=null;
    Workload workload=null;
    ExecutorService thread_pool =null;
    Long speedup = 0L;
    
    public DispatcherThread(String db_path,OpCounter opcounter,DB db, Workload workload,ExecutorService thread_pool,Properties props){
        try {
            this.bd_handler = new DBHandler(db_path);
            //bd_handler.init();
            
            replay = bd_handler.getReplay();
            replay_handler = replay.getPrimaryIndex(Long.class, Replay.class);
            
            this._db=db;
            _db.init();
            
            this.opscounter=opcounter;
            this.workload=workload;
            
            this.thread_pool = thread_pool;
            this._props=props;
            speedup = Long.parseLong(_props.getProperty(SPEEDUP_PROPERTY, SPEEDUP_PROPERTY_DEFAULT));
            speedup = 10000l;
            
        } catch (DatabaseException ex) {
            Logger.getLogger(DispatcherThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (DBException ex) {
            Logger.getLogger(DispatcherThread.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    
    @Override
    public void run() {
        try {
            ClientThreadState state = (ClientThreadState) workload.initThread(_props,1,1);
            ResultHandler resultHandler = state.getClient_resultHandler();
            
            EntityCursor<Replay> cursor = replay_handler.entities();
            
            Replay record = null;
            Replay record_next = null;
             
            while((record = cursor.next())!= null){
                record_next=cursor.next();
                
                if(record_next != null){
                    Long diff = (record_next.getTs()-record.getTs())/speedup;

                    thread_pool.execute(new WorkerThread(_db,record.getList(),diff,opscounter));
                    cursor.prev();
                }
                else{
                    //if it is the last entry to be replayed, dont sleep.
                    thread_pool.execute(new WorkerThread(_db,record.getList(),0l,opscounter));
                }
            }
            //Stop Thread Pool
            thread_pool.shutdown();
            thread_pool.awaitTermination(1, TimeUnit.MINUTES);
            
        } catch (DatabaseException ex) {
            Logger.getLogger(DispatcherThread.class.getName()).log(Level.SEVERE, null, ex);
        }  catch (WorkloadException ex) {
            Logger.getLogger(DispatcherThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(DispatcherThread.class.getName()).log(Level.SEVERE, null, ex);
        }        
    }
}

