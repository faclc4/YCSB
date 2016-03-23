package com.yahoo.ycsb;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.infinispan.versioning.rmi.RemoteVersionedCache;
import org.infinispan.versioning.rmi.RemoteVersionedCacheImpl;
import org.infinispan.versioning.utils.version.Version;
import org.infinispan.versioning.utils.version.VersionScalarGenerator;

/**
 * 
 * @author Fabio Coelho
 */
public class InfinispanGlueVersioningResultCheck extends DB {

  public static final int OK = 0;
  public static final int ERROR = -1;

  private boolean debug;
  private Object [] cache;
  private VersionScalarGenerator vsg = new VersionScalarGenerator();
  private int tcount;
  private Random rand;

  /**
   * This function is used to initiate the connection to ISPN an is executed once per client thread.
   */
  public void init() throws DBException {
    String serverList = super._p.getProperty("servers", "localhost:12345");
    String versioningTechnique = super._p.getProperty("versioningTechnique", "ATOMICMAP");
    
    tcount = Integer.parseInt(super._p.getProperty("threads", "1"));
    debug =super._p.getProperty("debug", "false").equals("true");

    rand = new Random(System.nanoTime());

    System.out.println("Versioning technique = "+versioningTechnique);
    System.out.println("RMI Servers = "+serverList);

    this.cache = new Object [tcount];
    
    for(int t = 0; t< tcount; t++){       
        //while(this.cache==null){
        String server = serverList.split(" ")[rand.nextInt(serverList.split(" ").length)];
        String serviceURL = "//" + server + "/"	+ RemoteVersionedCacheImpl.SERVICE_NAME + "-"	+ versioningTechnique;
        try{
            if(debug) System.out.print("Connecting to " + serviceURL + " ... ");
            this.cache[t] = (RemoteVersionedCache<String, String>) Naming.lookup(serviceURL);
            if(debug) System.out.println("[\u001b[1;44m OK \u001b[m]");
            if(super._p.getProperty("clear","false").equals("true")){
                System.out.print("Clearing cache ... ");
                ((RemoteVersionedCache<String, String>)this.cache[t]).clear();
                System.out.println("[\u001b[1;44m OK \u001b[m]");
            }

        }catch(Exception e){
            System.out.println("Fail to connect to "+server);
        }
        //System.out.println("cache initilly has: "+((RemoteVersionedCache<String, String>)this.cache[0]).getVersion()+" elements");
        // }
    }
  }

  @Override
  /**
     This function should implement a read
     Table: The cache to be used but it can be disregarded
     key: the key to be read. Key is selected according to distribution from the dump file.
     version: The version that should be considered when performing a read
  */
    public int read(String table, String key) {
    try {
        int r = rand.nextInt(tcount-1);
        String value = ((RemoteVersionedCache<String, String>)this.cache[r]).get(key);
        if (debug) System.out.println(key+" => "+value);
        return OK;
    } catch (RemoteException re) {
      re.printStackTrace();
      return ERROR;
    }
  }

  @Override
  /**
     This function should implement a readRange
     Table: The cache to be use but it can be overrided manually
     key: the key to be read. Key is selected according to distribution from the dump file.
     versionA and versionB: The versions that should be considered when performing a read range
  */
    public int readRange(String table, String key, Version versionA,Version versionB) {
    Version vA = this.vsg.increment(versionA);
    Version vB = this.vsg.increment(versionB);
    int r = rand.nextInt(tcount-1);
    try{
        if(debug) System.out.println(key+" (R "+vA.toString()+","+vB.toString()+") => ");
        Collection<Version> versions = ((RemoteVersionedCache<String, String>)this.cache[r]).get(key, vA, vB);
        if(debug) System.out.println(versions);
        return OK;
    } catch (RemoteException re) {
      re.printStackTrace();
      return ERROR;
    }
    
  }

  @Override
  /**
     This function should a implement a put into ISPN
     Table: can be the cache to use but it can de overrided manually.
     key: the key to be inserted. Key is constructed from the dump file.
     values: a Map of <version,value> to be inserted. version and values are constructed from the dump file.
  */
    public int update(String table, String key, HashMap<Version, ByteIterator> values) {
      int code = insert(table,key,values);
      return code;
  }

  @Override
  /**
     This function should implement an insert into ISPN during the LOAD stage of the benchmark.
     Table: can be the cache to use but it can de overrided manually.
     key: the key to be inserted. Key is constructed from the dump file.
     values: a Map of <version,value> to be inserted. version and values are constructed from the dump file.si
  */
    public int insert(String table, String key, HashMap<Version, ByteIterator> map) {
      int r = rand.nextInt(tcount-1);
      Map<Version, String> m = StringByteIterator.getVersionStringMap(map);
      if (debug) System.out.println(key+" (I) => ");
      try {
          ((RemoteVersionedCache<String, String>)this.cache[r]).putAll(key, m);
          return OK;
      } catch (RemoteException e) {
          e.printStackTrace();
          return ERROR;
      }
  }

  @Override
  /**
     DO NOT CONSIDER THIS FUNCTION- it will never be called.
  **/
    public int delete(String table, String key) {
    return OK;
  }

    @Override
    /**
      This function should implement a read to a given version.
     */
    public int read(String table, String key, Version versionA) {
        return OK;
    }

}
