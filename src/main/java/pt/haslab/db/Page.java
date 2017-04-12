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
