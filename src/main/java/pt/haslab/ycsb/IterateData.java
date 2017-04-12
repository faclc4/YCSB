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
package pt.haslab.ycsb;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import pt.haslab.db.DBHandler;
import pt.haslab.db.Operation;
import pt.haslab.db.Page;
import pt.haslab.db.Replay;

/**
 *
 * @author Fábio Coelho
 */
public class IterateData {
    
    public static void main(String[]args) throws DatabaseException{
        
        //First Argument should be the path to the DB environemnt.
        String db_path = args[0];
        DBHandler bd_handler = new DBHandler(db_path);
        
        EntityStore replay = bd_handler.getReplay();
        PrimaryIndex replay_handler = replay.getPrimaryIndex(Long.class, Replay.class);
        
        EntityCursor<Replay> cursor = replay_handler.entities();
            
        Replay record = null;
            
        while((record = cursor.next())!= null){
            for(Page pg : record.getList()){
                System.out.print("TS: "+record.getTs()+" Pages: "+pg.getUrl()+" : "+pg.getRevId()+" , ");
                if(pg.getop()==Operation.READ_LATEST){
                    System.out.println("READ_LATEST");
                }
                if(pg.getop()==Operation.READ_PREVIOUS){
                    System.out.println("READ_PREVIOUS");
                }
                if(pg.getop()==Operation.READ_RANGE){
                    System.out.println("READ_RANGE");
                }
            }
        }
    }
}
