package com.yahoo.db;

import com.sleepycat.persist.model.Persistent;

/**
 *
 * @author Fábio Coelho
 */
@Persistent public class Page {
    
    String url;
    Integer op;
    Long RevId;
    
    public Page(){}
    
    public Page(String url, Integer op, Long RevId){
        this.url=url;
        this.op=op;
        this.RevId=RevId;
    }
    
    public String getUrl(){
        return this.url;
    }
    
    public Integer getop(){
        return this.op;
    }
    
    public Long getRevId(){
        return this.RevId;
    }
    
}
