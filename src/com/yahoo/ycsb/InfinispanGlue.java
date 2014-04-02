package com.yahoo.ycsb;

import org.infinispan.versioning.rmi.RemoteVersionedCache;
import org.infinispan.versioning.rmi.RemoteVersionedCacheImpl;
import org.infinispan.versioning.utils.version.Version;
import org.infinispan.versioning.utils.version.VersionScalarGenerator;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.HashMap;

/**
 * 
 * @author Fabio Coelho
 */
public class InfinispanGlue extends DB {

  public static final int OK = 0;
  public static final int ERROR = -1;
  public static final boolean debug = false;

  private RemoteVersionedCache<String, String> cache;
  private VersionScalarGenerator vsg = new VersionScalarGenerator();
  
  /**
   * This function is used to intiate the connection to USPN.and executed once per client thread.
   */
  public void init() throws DBException {
    String servers = System.getProperty("servers","localhost:12345");
    String versioningTechnique = System.getProperty("versioningTechnique", "ATOMICMAP");
    try{
      String serviceURL = "//" + servers + "/"
	+ RemoteVersionedCacheImpl.SERVICE_NAME + "-"
	+ versioningTechnique;
      if (debug) System.out.println("Connecting to " + serviceURL + " ...");
      this.cache = (RemoteVersionedCache<String, String>) Naming.lookup(serviceURL);

    }catch(Exception e){
      throw new RuntimeException(e.getMessage());
    }

  }

  @Override
  /**
     This function should implement a read
     Table: The cache to be used but it can be disresgarded
     key: the key to be read. Key is selected according to distribution from the dump file.
     version: The version that should be considered when performing a read
  */
    public int read(String table, String key, Version version) {
    if (debug) System.out.println("Implement a read function to ISNP");
    try {
      String value = this.cache.get(key);
      // System.out.println("key is: " + key+" :"+value);
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
    if (debug) System.out.println("This is  Readrange");
    Version vA = this.vsg.increment(versionA);
    Version vB = this.vsg.increment(versionB);
    try{
      Collection<String> values = cache.get(key, vA, vB);
      System.out.println("key is: " + key+" ranging "+versionA.toString()+" - "+versionB.toString()+" :"+values.size());
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
    public int update(String table, String key, HashMap<Version, Object> values) {
//    if (debug) System.out.println("This is  update");
//    if (debug) System.out.println("key is: " + key);
//    if (debug) System.out.println("versions are: " + values.toString());
//    return OK;
      throw new RuntimeException("NYI");
  }

  @Override
  /**
     This function should a implement an insert into ISPN during the LOAD stage of the benchmark.
     Table: can be the cache to use but it can de overrided manually.
     key: the key to be inserted. Key is constructed from the dump file.
     values: a Map of <version,value> to be inserted. version and values are contructed from the dump file.
  */
    public int insert(String table, String key, HashMap<Version, Object> map) {
    if (debug) System.out.println("This is an insert");
    // System.out.println("key is: " + key);
    if (debug) System.out.println("versions:values map equals: " + map.toString());
    try {
        for(Version version : map.keySet()){
            this.cache.put(key, map.get(version).toString());
        }
    } catch (RemoteException e) {
        e.printStackTrace();  // TODO: Customise this generated block
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

}
