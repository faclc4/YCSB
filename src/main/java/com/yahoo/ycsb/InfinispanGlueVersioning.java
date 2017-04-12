package com.yahoo.ycsb;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.*;
import org.infinispan.versioning.rmi.RemoteVersionedCache;
import org.infinispan.versioning.rmi.RemoteVersionedCacheImpl;
import org.infinispan.versioning.utils.version.Version;
import org.infinispan.versioning.utils.version.VersionScalarGenerator;

public class InfinispanGlueVersioning extends DB {

  public static final int OK = 0;
  public static final int ERROR = -1;

  private boolean debug;
  private RemoteVersionedCache<String, String> cache;
  private VersionScalarGenerator vsg = new VersionScalarGenerator();

  /**
   * This function is used to initiate the connection to USPN.and executed once per client thread.
   */
  public void init() throws DBException {
    String serverList = super._p.getProperty("servers", "localhost:12345");
    String versioningTechnique = super._p.getProperty("versioningTechnique", "ATOMICMAP");
    debug =super._p.getProperty("debug", "false").equals("true");

    Random rand = new Random(System.nanoTime());

    System.out.println("Versioning technique = "+versioningTechnique);
    System.out.println("RMI Servers = "+serverList);

    while(this.cache==null){
        String server = serverList.split(" ")[rand.nextInt(serverList.split(" ").length)];
        String serviceURL = "//" + server + "/"	+ RemoteVersionedCacheImpl.SERVICE_NAME + "-"	+ versioningTechnique;
        try{
            if(debug) System.out.print("Connecting to " + serviceURL + " ... ");
            this.cache = (RemoteVersionedCache<String, String>) Naming.lookup(serviceURL);
            if(debug) System.out.println("[\u001b[1;44m OK \u001b[m]");
            if(super._p.getProperty("clear","false").equals("true")){
                System.out.print("Clearing cache ... ");
                this.cache.clear();
                System.out.println("[\u001b[1;44m OK \u001b[m]");
            }
        }catch(Exception e){
            System.out.println("Fail to connect to "+server);
        }
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
      String value = this.cache.get(key);
        if (debug) System.out.println(key+" => "+value);
    } catch (RemoteException re) {
      re.printStackTrace();
      return ERROR;
    }
    return OK;
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
    try{
        if(debug) System.out.println(key+" (R "+vA.toString()+","+vB.toString()+") => ");
        Collection<Version> versions = cache.get(key, vA, vB);
        if(debug) System.out.println(versions);
    } catch (RemoteException re) {
      re.printStackTrace();
      return ERROR;
    }
    return OK;
  }

  @Override
  /**
     This function should a implement a put into ISPN
     Table: can be the cache to use but it can de overrided manually.
     key: the key to be inserted. Key is constructed from the dump file.
     values: a Map of <version,value> to be inserted. version and values are contructed from the dump file.
  */
    public int update(String table, String key, HashMap<Version, ByteIterator> values) {
      Map<Version, String> m = StringByteIterator.getVersionStringMap(values);
      if (debug) System.out.println(key+" (U) => " + m);
      try {
          this.cache.putAll(key,m);
      } catch (RemoteException e) {
          e.printStackTrace();
          return ERROR;
      }

      return OK;
  }

  @Override
  /**
     This function should a implement an insert into ISPN during the LOAD stage of the benchmark.
     Table: can be the cache to use but it can de overrided manually.
     key: the key to be inserted. Key is constructed from the dump file.
     values: a Map of <version,value> to be inserted. version and values are contructed from the dump file.
  */
    public int insert(String table, String key, HashMap<Version, ByteIterator> map) {
      Map<Version, String> m = StringByteIterator.getVersionStringMap(map);
      if (debug) System.out.println(key+" (I) => " + m);
      try {
          this.cache.putAll(key,m);
      } catch (RemoteException e) {
          e.printStackTrace();
          return ERROR;
      }
      return OK;
  }

  @Override
  /**
     DO NOT CONSIDER THIS FUNCTION- it will never be called.
  **/
    public int delete(String table, String key) {
    return OK;
  }

    @Override
    public int read(String table, String key, Version versionA) {
        return OK;
    }
}
