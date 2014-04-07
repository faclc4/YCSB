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

import org.infinispan.versioning.utils.version.Version;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;


/**
 * Basic DB that just prints out the requested operations, instead of doing them against a database.
 */
public class BasicDB extends DB
{
	public static final String VERBOSE="basicdb.verbose";
	public static final String VERBOSE_DEFAULT="true";
	
	public static final String SIMULATE_DELAY="basicdb.simulatedelay";
	public static final String SIMULATE_DELAY_DEFAULT="0";
	
	
	boolean verbose;
	int todelay;

	public BasicDB()
	{
		todelay=0;
	}

	
	void delay()
	{
		if (todelay>0)
		{
			try
			{
				Thread.sleep((long)Utils.random().nextInt(todelay));
			}
			catch (InterruptedException e)
			{
				//do nothing
			}
		}
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	@SuppressWarnings("unchecked")
	public void init()
	{
		verbose=Boolean.parseBoolean(getProperties().getProperty(VERBOSE, VERBOSE_DEFAULT));
		todelay=Integer.parseInt(getProperties().getProperty(SIMULATE_DELAY, SIMULATE_DELAY_DEFAULT));
		
		if (verbose)
		{
			System.out.println("***************** properties *****************");
			Properties p=getProperties();
			if (p!=null)
			{
				for (Enumeration e=p.propertyNames(); e.hasMoreElements(); )
				{
					String k=(String)e.nextElement();
					System.out.println("\""+k+"\"=\""+p.getProperty(k)+"\"");
				}
			}
			System.out.println("**********************************************");
		}
	}

	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	public int delete(String table, String key)
	{
		delay();

		if (verbose)
		{
			System.out.println("DELETE "+table+" "+key);
		}

		return 0;
	}

    @Override
    public int read(String table, String key) {
        delay();

		if (verbose)
		{
			System.out.print("READ "+table+" "+key+" [ ");
			System.out.println("]");
		}

	return 0;
    }

    @Override
    public int readRange(String table, String key, Version versionA, Version versionB) {
        delay();

		if (verbose)
		{
			System.out.print("READ "+table+" "+key+" [ ");
			if (versionA!=null && versionB != null)
			{
                            System.out.print(versionA+" "+versionB+" ");
			}

			System.out.println("]");
		}

	return 0;
    }

    @Override
    public int update(String table, String key, HashMap<Version, ByteIterator> values) {
        delay();

		if (verbose)
		{
			System.out.print("UPDATE "+table+" "+key+" [ ");
			if (values!=null)
			{
				for (Object k : values.keySet())
				{
					System.out.print(k.toString()+"="+values.get(k.toString())+" ");
				}
			}
			System.out.println("]");
		}

		return 0;
    }

    @Override
    public int insert(String table, String key, HashMap<Version, ByteIterator> values) {
        delay();

		if (verbose)
		{
			System.out.print("INSERT "+table+" "+key+" [ ");
			if (values!=null)
			{
				for (Object k : values.keySet())
				{
					System.out.print(k.toString()+"="+values.get(k.toString())+" ");
				}
			}

			System.out.println("]");
		}

		return 0;
    }

	/**
	 * Short test of BasicDB
	 */
	/*
	public static void main(String[] args)
	{
		BasicDB bdb=new BasicDB();

		Properties p=new Properties();
		p.setProperty("Sky","Blue");
		p.setProperty("Ocean","Wet");

		bdb.setProperties(p);

		bdb.init();

		HashMap<String,String> fields=new HashMap<String,String>();
		fields.put("A","X");
		fields.put("B","Y");

		bdb.read("table","key",null,null);
		bdb.insert("table","key",fields);

		fields=new HashMap<String,String>();
		fields.put("C","Z");

		bdb.update("table","key",fields);

		bdb.delete("table","key");
	}*/
}
