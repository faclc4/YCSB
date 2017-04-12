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

import java.util.*;
import org.infinispan.versioning.rmi.RemoteVersionedCache;

import org.infinispan.versioning.utils.version.Version;
import org.infinispan.versioning.utils.version.VersionScalarGenerator;

/**
 * 
 * @author Fabio Coelho
 */
public class InfinispanGlueRaw extends DB {

  public static final int OK = 0;
  public static final int ERROR = -1;

  private boolean debug;
  private RemoteVersionedCache<String, String> cache;
  private VersionScalarGenerator vsg = new VersionScalarGenerator();

  /**
   * This function is used to intiate the connection to USPN.and executed once per client thread.
   */
  @Override
  public void init() throws DBException {
    System.out.println("Raw populate");

  }

  @Override
  /**
     This function should implement a read
     Table: The cache to be used but it can be disresgarded
     key: the key to be read. Key is selected according to distribution from the dump file.
     version: The version that should be considered when performing a read
  */
    public int read(String table, String key) {
        //System.out.println("Read Op => key: "+key);
        System.out.println(key);
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
        //System.out.println("ReadRange Op => key: "+key+" versionA: "+versionA.toString()+" ,versionB: "+versionB.toString());
        System.out.println(key);
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
      //System.out.println("Upd Op => key: "+key);  
      System.out.println(key);
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
      //System.out.println("Insert Op => key: "+key);  
      //System.out.println(key);
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
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        System.out.println(key);
        return OK;
    }
}
