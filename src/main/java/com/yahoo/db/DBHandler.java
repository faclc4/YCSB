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
    EntityStore replay_ts_pageID;
    EntityStore pageId_Access_log;
    EntityStore dump;
    EntityStore replay;
    
    private String db_path;
    private String db_path_final;
    private EnvironmentConfig env_config_final;
    private Environment env_final;
  
    public DBHandler(String path){
        this.db_path=path;
        this.init_single();
    }
    
    public DBHandler(String path, String path_final){
        this.db_path=path;
        this.db_path_final=path_final;
        this.init_double();
    }
    
    public DBHandler(){
        //this.db_path="/home/fabio/Documents/Replayer/YCSB_replayer/dbs";
        this.db_path="";
    }
    
    
    public void init_single(){
        try {
            env_config = new EnvironmentConfig();
            env_config.setAllowCreate(true);
            //env_config.setCacheSize(500000000);
            env_config.setConfigParam("je.log.fileMax","250000000");
            
            //Environment = SQL Database;
            env = new Environment(new File(db_path),env_config);
 
            StoreConfig stConfig = new StoreConfig();
            stConfig.setAllowCreate(true);
            
            replay_ts_pageID = new EntityStore(env,"replay_ts_pageID", stConfig);
            pageId_Access_log = new EntityStore(env,"pageId_Access_log", stConfig);
            dump = new EntityStore(env,"dump",stConfig);
            replay = new EntityStore(env,"replay",stConfig);
        } catch (DatabaseException ex) {
            Logger.getLogger(DBHandler.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void init_double(){
        try {
            env_config = new EnvironmentConfig();
            env_config.setAllowCreate(true);

            env_config_final = new EnvironmentConfig();
            env_config_final.setAllowCreate(true);

            //Environment = SQL Database;
            env = new Environment(new File(db_path),env_config);
            env_final = new Environment(new File(db_path_final),env_config_final);

            StoreConfig stConfig = new StoreConfig();
            stConfig.setAllowCreate(true);

            replay_ts_pageID = new EntityStore(env,"replay_ts_pageID", stConfig);
            pageId_Access_log = new EntityStore(env,"pageId_Access_log", stConfig);
            dump = new EntityStore(env,"dump",stConfig);
            replay = new EntityStore(env_final,"replay",stConfig);
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
