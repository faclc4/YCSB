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

