package com.yahoo.db;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;
import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Fábio Coelho
 */
public class DBHandler {
    
    EnvironmentConfig env_config;
    Environment env;
    DatabaseConfig db_conf;
    //Database replay_ts_pageID;
    //Database pageId_Access_log;
    //Database dump;
    
    
    EntityStore replay_ts_pageID;
    EntityStore pageId_Access_log;
    EntityStore dump;
    EntityStore replay;
    
    private String db_path;
  
    public DBHandler(String path){
        this.db_path=path;
        this.init();
    }
    
    public DBHandler(){
        //this.db_path="/home/fabio/Documents/Replayer/YCSB_replayer/dbs";
    }
    
    public void init(){
        try {
            env_config = new EnvironmentConfig();
            env_config.setAllowCreate(true);
            
            //Environment = SQL Database;
            env = new Environment(new File(db_path),env_config);
            
            StoreConfig stConfig = new StoreConfig();
            stConfig.setAllowCreate(true);
            
            /*
            db_conf = new DatabaseConfig();
            db_conf.setAllowCreate(true);
            
            replay_ts_pageID = env.openDatabase(null, "replay_ts_pageID", db_conf);
            pageId_Access_log = env.openDatabase(null, "pageId_Access_log", db_conf);
            dump = env.openDatabase(null, "dump", db_conf);
            */
            
            replay_ts_pageID = new EntityStore(env,"replay_ts_pageID", stConfig);
            pageId_Access_log = new EntityStore(env,"pageId_Access_log", stConfig);
            dump = new EntityStore(env,"dump",stConfig);
            replay = new EntityStore(env,"replay",stConfig);
        } catch (DatabaseException ex) {
            Logger.getLogger(DBHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void closeConn(){
        try {
            env.close();
        } catch (DatabaseException ex) {
            Logger.getLogger(DBHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }    
    
    public EntityStore getReplay_ts_pageID(){
        return this.replay_ts_pageID;
    }
    
    public EntityStore getpageId_Acess_log(){
        return this.pageId_Access_log;
    }
    
    public EntityStore getDump(){
        return this.dump;
    }
    
    public EntityStore getReplay(){
        return this.replay;
    }
}
