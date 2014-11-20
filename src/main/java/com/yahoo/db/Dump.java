package com.yahoo.db;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;

/**
 *
 * @author Fábio Coelho
 */
@Entity
public class Dump {
    
    @PrimaryKey
    String pageId;
    
    Map<Long,Long> revisionId_size = null;
    
    public Dump(){
    
    }
    
    public Dump(String pageId){
        this.pageId=pageId;
        this.revisionId_size = new TreeMap<Long,Long>();
    }
    
    public Dump(String pageId, Long revId, Long size){
        this.pageId=pageId;
        this.revisionId_size = new TreeMap<Long,Long>();
        this.revisionId_size.put(revId, size);
    }
    
    public Dump(String pageId, Map<Long,Long> map){
        this.pageId=pageId;
        this.revisionId_size=map;
    }
    
    public boolean contains(Long revId){
        if(this.revisionId_size.containsKey(revId))
            return true;
        else return false;
    }
    
    public void set_newRev(Long revId, Long size){
        if (!contains(revId)){
            this.revisionId_size.put(revId, size);
        }
    }
    
    public String getPageId(){
        return this.pageId;
    }
    
    public Long getSize(Long revId){
        if (contains(revId)){
            return this.revisionId_size.get(revId);
        }
        else return 0L;
    }
    
    public Map<Long,Long> getMap(){
        return this.revisionId_size;
    }
    
    public Long getLastMapIndex(){
        //return this.revisionId_size.get(this.revisionId_size.size()-1);
        return (new ArrayList<Long>(this.revisionId_size.values())).get(this.revisionId_size.size()-1);
    }
    
    public Long getFirstMapIndex(){
        return (new ArrayList<Long>(this.revisionId_size.values())).get(0);
    }
}
