package com.yahoo.db;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Fábio Coelho
 */
@Entity
public class Replay {
    
    @PrimaryKey
    Long ts;
    
    List<Page> list = new ArrayList<Page>();
    
    public Replay(Long ts, String url, Integer op, Long RevId){
        this.ts=ts;
        this.list.add(new Page(url,op,RevId));
    }
    
    public Replay(Long ts, List<Page> list){
        this.ts=ts;
        this.list=list;
    }
    
    public Replay(){
    
    }
    
    public List<Page> getList(){
        return this.list;
    }
    
    public Long getTs(){
        return this.ts;
    }
}
