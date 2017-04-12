/*
 * Copyright 2016 by INESC TEC                                               
 * Developed by Fábio Coelho                                                 
 * This work was based on the YCSB Project from Yahoo!                          
 *
 * Licensed under the Apache License, Version 2.0 (the "License");           
 * you may not use this file except in compliance with the License.          
 * You may obtain a copy of the License at                                   
 *
 * http://www.apache.org/licenses/LICENSE-2.0                              
 *
 * Unless required by applicable law or agreed to in writing, software       
 * distributed under the License is distributed on an "AS IS" BASIS,         
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  
 * See the License for the specific language governing permissions and       
 * limitations under the License.                                            
 */
package pt.haslab.db;

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
