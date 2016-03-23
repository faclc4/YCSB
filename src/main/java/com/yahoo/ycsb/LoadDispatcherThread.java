package com.yahoo.ycsb;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.yahoo.db.DBHandler;
import com.yahoo.db.Dump;
import com.yahoo.ycsb.measurements.ResultHandler;
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