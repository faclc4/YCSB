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

import java.util.Properties;

/**
 * Creates a DB layer by dynamically classloading the specified DB class.
 */
public class DBFactory
{
      @SuppressWarnings("unchecked")
	public static DB newDB(String dbname, Properties properties) throws UnknownDBException
      {
	 ClassLoader classLoader = DBFactory.class.getClassLoader();

	 DB ret=null;

	 try 
	 {
	    Class dbclass = classLoader.loadClass(dbname);
	    ret=(DB)dbclass.newInstance();
	 }
	 catch (Exception e) 
	 {  
	    e.printStackTrace();
	    return null;
	 }
	 
	 ret.setProperties(properties);

	 return new DBWrapper(ret);
      }
      
}
