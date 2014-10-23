package com.yahoo.db;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;

/**
 *
 * @author Fábio Coelho
 */
@Entity
public class Page_id_AcessLog {
    
    @PrimaryKey
    String pageID;
    
    Long firt_ts;
    Long Last_ts;
    
    public Page_id_AcessLog(){
        
    }
    
    public Page_id_AcessLog(String pageId,Long first,Long last){
        this.pageID=pageId;
        this.firt_ts = first;
        this.Last_ts = last;
    }
    
    public Long getfirst(){
        return this.firt_ts;
    }
    
    public Long getLast(){
        return this.Last_ts;
    }
    
    public void setLast(Long last){
        this.Last_ts=last;
    }
    
    public void setFirst(Long first){
        this.firt_ts=first;
    }
}

