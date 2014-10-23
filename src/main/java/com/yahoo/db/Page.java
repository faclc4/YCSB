package com.yahoo.db;

/**
 *
 * @author Fábio Coelho
 */
public class Page {
    
    String url;
    Integer op;
    Long RevId;
    
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
