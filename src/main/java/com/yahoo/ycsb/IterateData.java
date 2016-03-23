package com.yahoo.ycsb;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.persist.EntityCursor;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.PrimaryIndex;
import com.yahoo.db.DBHandler;
import com.yahoo.db.Operation;
import com.yahoo.db.Page;
import com.yahoo.db.Replay;

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
