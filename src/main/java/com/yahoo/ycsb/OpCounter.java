package com.yahoo.ycsb;

/**
 *
 * @author Fábio Coelho
 */
public class OpCounter {
    
    private int ops;
    
    public OpCounter(){
        this.ops=0;
    }
    
    public int getOppDone(){
        return this.ops;
    }
    
    public void incrementOp(){
        this.ops++;
    }
    
    public void resetOpsCounter(){
        this.ops=0;
    }
    
}
