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

import pt.haslab.ycsb.measurements.ResultHandler;

/**
 *
 * @author Fábio Coelho
 */
public class ClientThreadState {
    ResultHandler client_resultHandler;
    boolean scan_thread;

    public ClientThreadState(ResultHandler client_resultHandler) {
        this.client_resultHandler = client_resultHandler;
        this.scan_thread = false;
    }

    public void setScan_thread(boolean scan_thread) {
        this.scan_thread = scan_thread;
    }


    public ResultHandler getClient_resultHandler() {
        return client_resultHandler;
    }

    public boolean isScan_thread() {
        return scan_thread;
    }
}
