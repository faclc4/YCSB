package com.yahoo.ycsb;

import java.util.HashMap;
import org.infinispan.versioning.utils.version.Version;
/**
 *
 * @author Fábio Coelho
 */
public class InfinispanGlue extends DB{
    
    public static final int OK=0;
    public static final int ERROR=-1;

    /**
    This function is used to intiate the connection to the DB
    This function is executed once per client thread.
    */    
    public void init() throws DBException{
	System.out.println("Client started");
        
    }
    

    @Override
    /**
    This function should implement a read
    Table: can be the cache to use but it can be overrided manually
    key: the key to be read. Key is selected according to distribution from the dump file.
    version: The version that should be considered when performing a read
    */
    public int read(String table, String key){
	System.out.println("This is  Read");
	System.out.println("key is: "+key);
        return OK;
    }

    @Override
    /**
    This function should implement a readRange in the DB
    Table: can be the cache to use but it can be overrided manually
    key: the key to be read. Key is selected according to distribution from the dump file.
    versionA and versionB: The versions that should be considered when performing a read range
    */
    public int readRange(String table, String key, Version versionA, Version versionB){
        System.out.println("This is  Readrange");
        System.out.println("key is: "+key+ " versionA: "+versionA.toString()+" versionB: "+versionB.toString());
        return OK;
    }

   

        /**
    This function should a implement a put into the DB.
    Table: can be the cache to use but it can de overrided manually.
    key: the key to be inserted. Key is constructed from the dump file.
    values: a Map of <version,value> to be inserted. version and values are contructed from the dump file.
    */
    public int update(String table, String key, HashMap<Version,ByteIterator> values){
	System.out.println("This is  update");
	System.out.println("Key is: "+key);
	System.out.println("versions are: "+values.toString());
	return OK;
    }

    @Override
    public int insert(String table, String key, HashMap<Version,ByteIterator> values){
	System.out.println("This is a insert");
	System.out.println("key is: "+key);
	System.out.println("versions are: "+values.toString());
        return OK;
    }

    @Override
    public int delete(String table, String key) {
        return OK;
    }

    @Override
    public int read(String table, String key, Version versionA) {
        System.out.println("This is  Readrange");
        System.out.println("key is: "+key+ " version: "+versionA.toString());
        return OK;
    }

  
    
}
