package com.yahoo.ycsb;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.yahoo.db.DBHandler;
import com.yahoo.db.Dump;
import com.yahoo.db.Page_id_AcessLog;
import com.yahoo.ycsb.measurements.ResultHandler;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.infinispan.versioning.utils.version.Version;
import org.infinispan.versioning.utils.version.VersionScalar;

/**
 *
 * @author Fábio Coelho
 */
public class LoaderThread implements Runnable{
       
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
    
    private final String table="usertable";
    
    
    public LoaderThread(String db_path,OpCounter opcounter,DB db, Workload workload,ExecutorService thread_pool,Properties props){
        try {
            this.bd_handler = new DBHandler(db_path);
            
            dump = bd_handler.getDump();
            dump_handler = dump.getPrimaryIndex(String.class, Dump.class);
            
            this._db=db;
            _db.init();
            
            this.opscounter=opcounter;
            this.workload=workload;
            
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
                       
            EntityCursor<Dump> cursor = dump_handler.entities();
            
            Dump record = null;
            HashMap<Version,ByteIterator> content = new HashMap<>();
            
            while((record = cursor.next())!= null){
               for(Map.Entry<Long,Long> revision : record.getMap().entrySet()){
                   content.put(new VersionScalar(revision.getKey()), new RandomByteIterator(revision.getValue()));
               }
               _db.insert(table, record.getPageId(), content);
            }
            
        } catch (DatabaseException ex) {
            Logger.getLogger(DispatcherThread.class.getName()).log(Level.SEVERE, null, ex);
        } catch (WorkloadException ex) {
            Logger.getLogger(DispatcherThread.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }
    
    
    
}
