package com.yahoo.ycsb;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.yahoo.db.DBHandler;
import com.yahoo.db.Page;
import com.yahoo.db.Replay;
import com.yahoo.ycsb.measurements.ResultHandler;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Fábio Coelho
 */
public class DispatcherThread implements Runnable{
//This single thread reads the replay database and instructs worker threads to perform actions.
    
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
            
            //record != cursor.last()
            while((record = cursor.next())!= null){
                record_next = cursor.next();
                if(record_next != null){
                    Long diff = record_next.getTs()-record.getTs();
                
                    //Submits each page op to the threadpool.
                
                    for(Page pg : record.getList()){
                        thread_pool.execute(new WorkerThread(_db,pg,resultHandler));
                        opscounter.incrementOp();
                    }                
                    cursor.prev();
                    //Sleeps the required time;
                    //Thread.sleep(diff);
                }
                else{
                    //Submits each page op to the threadpool.
                    for(Page pg : record.getList()){
                        thread_pool.execute(new WorkerThread(_db,pg,resultHandler));
                        opscounter.incrementOp();
                    }
                }
            }
            //Stop Thread Pool
            thread_pool.shutdown();
            
        } catch (DatabaseException ex) {
            Logger.getLogger(DispatcherThread.class.getName()).log(Level.SEVERE, null, ex);
        }  catch (WorkloadException ex) {
            Logger.getLogger(DispatcherThread.class.getName()).log(Level.SEVERE, null, ex);
        }        
    }
}

