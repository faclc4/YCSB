package com.yahoo.db;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author Fábio Coelho
 */
@Entity
public class Replay_ts_pageID {
    
    @PrimaryKey
    Long ts;
    
    Map<String,Integer> pages;
    
    public Replay_ts_pageID(){
        
    }
    
    public Replay_ts_pageID(Long ts,String pageID,Integer op){
        this.ts=ts;
        this.pages= new TreeMap<String,Integer>();
        this.pages.put(pageID, op);
    }
    
    public Replay_ts_pageID(Long ts, Map<String,Integer> map){
        this.ts=ts;
        this.pages=map;
    }
    
    public Map<String, Integer> getMap(){
        return this.pages;
    }
    
    public Long getTs(){
        return this.ts;
    }
}
