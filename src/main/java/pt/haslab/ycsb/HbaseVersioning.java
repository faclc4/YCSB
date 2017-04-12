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


import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.infinispan.versioning.utils.version.Version;
import org.infinispan.versioning.utils.version.VersionScalarGenerator;

/**
 * 
 * @author Fabio Coelho
 */
public class HbaseVersioning extends DB {

  public static final int OK = 0;
  public static final int ERROR = -1;

  private boolean debug;
  private Object [] cache;
  private VersionScalarGenerator vsg = new VersionScalarGenerator();
  private int tcount;
  private Random rand;

    private static final Configuration config = HBaseConfiguration.create(); //new HBaseConfiguration();
    private static final AtomicInteger THREAD_COUNT = new AtomicInteger(0);

    public boolean _debug=true;

    public String _table="";
    private static HConnection _hConn=null;
    public HTableInterface _hTable=null;
    public String _columnFamily="";
    public byte _columnFamilyBytes[];
    public boolean _clientSideBuffering = false;
    public long _writeBufferSize = 1024 * 1024 * 12;
    /** Whether or not a page filter should be used to limit scan length. */
    public boolean _usePageFilter = true;

    public static final int HttpError=-2;

    public static final Object tableLock = new Object();
    
  
  /**
   * This function is used to initiate the connection to ISPN an is executed once per client thread.
   */
  @Override
      public void init() throws DBException
    {
        try {
            THREAD_COUNT.getAndIncrement();
            synchronized(THREAD_COUNT) {
              if (_hConn == null){
                _hConn = HConnectionManager.createConnection(config);
              }
            }
        } catch (IOException e) {
            System.err.println("Connection to HBase was not successful");
            throw new DBException(e);  
        }
        _columnFamily = "ispn";
        if (_columnFamily == null)
        {
            System.err.println("Error, must specify a columnfamily for HBase table");
            throw new DBException("No columnfamily specified");
        }
      _columnFamilyBytes = Bytes.toBytes(_columnFamily);

      // Terminate right now if table does not exist, since the client
      // will not propagate this error upstream once the workload
      // starts.
      String table = pt.haslab.ycsb.workloads.File_CoreWorkload.table;
      try
	  {
	      HTableInterface ht = _hConn.getTable(table);
	      ht.getTableDescriptor();
	  }
      catch (IOException e)
	  {
	      throw new DBException(e);
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
        HashMap<String,ByteIterator> result = new HashMap<String,ByteIterator>();
        //if this is a "new" table, init HTable object.  Else, use existing one
       
        if (!_table.equals(table)) {
            _hTable = null;
            try
            {
                getHTable(table);
                _table = table;
            }
            catch (IOException e)
            {
                System.err.println("Error accessing HBase table: "+e);
                return ERROR;
            }
        }

        Result r = null;
        try
        {
        if (_debug) {
        System.out.println("Doing read from HBase columnfamily "+_columnFamily);
        System.out.println("Doing read for key: "+key);
        }
            Get g = new Get(Bytes.toBytes(key));
            g.addFamily(_columnFamilyBytes);

            r = _hTable.get(g);
        }
        catch (IOException e)
        {
            System.err.println("Error doing get: "+e);
            return ERROR;
        }
        catch (ConcurrentModificationException e)
        {
            //do nothing for now...need to understand HBase concurrency model better
            return ERROR;
        }

        for (KeyValue kv : r.raw()) {
          result.put(
              Bytes.toString(kv.getQualifier()),
              new ByteArrayByteIterator(kv.getValue()));
          if (_debug) {
            System.out.println("Result for field: "+Bytes.toString(kv.getQualifier())+
                " is: "+Bytes.toString(kv.getValue()));
          }

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
        
        Vector<HashMap<String,ByteIterator>> result = new Vector<HashMap<String,ByteIterator>>();
    
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try
            {
                getHTable(table);
                _table = table;
            }
            catch (IOException e)
            {
                System.err.println("Error accessing HBase table: "+e);
                return ERROR;
            }
        }

        Scan s = new Scan(Bytes.toBytes(vA.toString()));
        //HBase has no record limit.  Here, assume recordcount is small enough to bring back in one call.
        //We get back recordcount records
        //s.setCaching(recordcount);
        //if (this._usePageFilter) {
        //  s.setFilter(new PageFilter(recordcount));
        //}

        //add specified fields or else all fields

            s.addFamily(_columnFamilyBytes);



        //get results
        ResultScanner scanner = null;
        try {
            scanner = _hTable.getScanner(s);
            int numResults = 0;
            for (Result rr = scanner.next(); rr != null; rr = scanner.next())
            {
                //get row key
                String k = Bytes.toString(rr.getRow());
                if (_debug)
                {
                    System.out.println("Got scan result for key: "+k);
                }

                HashMap<String,ByteIterator> rowResult = new HashMap<String, ByteIterator>();

                for (KeyValue kv : rr.raw()) {
                  rowResult.put(
                      Bytes.toString(kv.getQualifier()),
                      new ByteArrayByteIterator(kv.getValue()));
                }
                //add rowResult to result vector
                result.add(rowResult);
                numResults++;

                // PageFilter does not guarantee that the number of results is <= pageSize, so this
                // break is required.
            } //done with row

        }

        catch (IOException e) {
            if (_debug)
            {
                System.out.println("Error in getting/parsing scan result: "+e);
            }
            return ERROR;
        }

        finally {
            scanner.close();
        }

        return OK;
    }

  /**
     This function should a implement a put into ISPN
     Table: can be the cache to use but it can de overrided manually.
     key: the key to be inserted. Key is constructed from the dump file.
     values: a Map of <version,value> to be inserted. version and values are constructed from the dump file.
  */
  @Override
    public int update(String table, String key, HashMap<Version, ByteIterator> values) {
      insert(table,key,values);

      return OK;
  }

  /**
     This function should implement an insert into ISPN during the LOAD stage of the benchmark.
     Table: can be the cache to use but it can de overrided manually.
     key: the key to be inserted. Key is constructed from the dump file.
     values: a Map of <version,value> to be inserted. version and values are constructed from the dump file.si
  */
  @Override
    public int insert(String table, String key, HashMap<Version, ByteIterator> map) {
        //if this is a "new" table, init HTable object.  Else, use existing one
        if (!_table.equals(table)) {
            _hTable = null;
            try
            {
                getHTable(table);
                _table = table;
            }
            catch (IOException e)
            {
                System.err.println("Error accessing HBase table: "+e);
                return ERROR;
            }
        }


        if (_debug) {
            System.out.println("Setting up put for key: "+key);
        }
        Put p = new Put(Bytes.toBytes(key));
        for (Map.Entry<Version, ByteIterator> entry : map.entrySet())
        {
            byte[] value = entry.getValue().toArray();
            if (_debug) {
                if(value.equals(null)){
                    System.out.println("Adding field/value " + entry.getKey() + "/ VALUE IS NULL to put request");
                }
            }
            p.add(_columnFamilyBytes,Bytes.toBytes(entry.getKey().toString()), value);
        }

        try
        {
            System.out.println(p);
            
            _hTable.put(p);
            Thread.sleep(10);
        }
        catch (IOException e)
        {
            if (_debug) {
                System.err.println("Error doing put: "+e);
            }
            return ERROR;
        }
        catch (ConcurrentModificationException e)
        {
            //do nothing for now...hope this is rare
            return ERROR;
        } catch (InterruptedException ex) {
          Logger.getLogger(HbaseVersioning.class.getName()).log(Level.SEVERE, null, ex);
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
    /**
      This function should implement a read to a given version.
     */
    public int read(String table, String key, Version versionA) {
        return OK;
    }
    
    public void getHTable(String table) throws IOException
    {
        synchronized (tableLock) {
            _hTable = _hConn.getTable(table);
            //_hTable.setAutoFlush(!_clientSideBuffering, true);
            //_hTable.setWriteBufferSize(_writeBufferSize);
            //return hTable;
        }

    }

}
