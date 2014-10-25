/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb;

import com.yahoo.ycsb.measurements.Measurements;
import org.infinispan.versioning.utils.version.Version;

import java.util.HashMap;
import java.util.Properties;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 */
public class DBWrapper extends DB
{
	DB _db;
	Measurements _measurements;

	public DBWrapper(DB db)
	{
		_db=db;
		_measurements=Measurements.getMeasurements();
	}

	/**
	 * Set the properties for this DB.
	 */
	public void setProperties(Properties p)
	{
		_db.setProperties(p);
	}

	/**
	 * Get the set of properties for this DB.
	 */
	public Properties getProperties()
	{
		return _db.getProperties();
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init() throws DBException
	{
		_db.init();
	}

	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
		_db.cleanup();
	}



    @Override
    public int read(String table, String key) {
        long st=System.nanoTime();
	int res=_db.read(table,key);
	long en=System.nanoTime();
        int time = (int)((en-st)/1000);
        time = (time<0) ? Integer.MAX_VALUE : time;
	_measurements.measure("READ", time);
	_measurements.reportReturnCode("READ",res);
	return res;
    }

    @Override
    public int readRange(String table, String key, Version versionA, Version versionB) {
              long st=System.nanoTime();
	int res=_db.readRange(table,key,versionA,versionB);
	long en=System.nanoTime();
        int time = (int)((en-st)/1000);
        time = (time<0) ? Integer.MAX_VALUE : time;
	_measurements.measure("READ_RANGE", time);
	_measurements.reportReturnCode("READ_RANGE",res);
	return res;
    }

    @Override
    public int update(String table, String key, HashMap<Version, ByteIterator> values) {
        long st=System.nanoTime();
	int res=_db.update(table,key,values);
	long en=System.nanoTime();
	_measurements.measure("UPDATE",(int)((en-st)/1000));
	_measurements.reportReturnCode("UPDATE",res);
	return res;
    }

    @Override
    public int insert(String table, String key, HashMap<Version, ByteIterator> values) {
        long st=System.nanoTime();
	int res=_db.insert(table,key,values);
	long en=System.nanoTime();
	_measurements.measure("INSERT",(int)((en-st)/1000));
	_measurements.reportReturnCode("INSERT",res);
	return res;
    }

    @Override
    public int delete(String table, String key) {
        long st=System.nanoTime();
	int res=_db.delete(table,key);
	long en=System.nanoTime();
	_measurements.measure("DELETE",(int)((en-st)/1000));
	_measurements.reportReturnCode("DELETE",res);
	return res;
    }

    @Override
    public int read(String table, String key, Version versionA) {
        long st=System.nanoTime();
        int res=_db.read(table,key);
	long en=System.nanoTime();
        int time = (int)((en-st)/1000);
        time = (time<0) ? Integer.MAX_VALUE : time;
	_measurements.measure("READ_PREVIOUS", time);
	_measurements.reportReturnCode("READ_PREVIOUS",res);
	return res;
    }
}
