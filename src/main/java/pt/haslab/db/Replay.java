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
