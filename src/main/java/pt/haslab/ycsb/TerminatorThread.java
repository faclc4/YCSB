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

import java.util.Vector;

/**
 * A thread that waits for the maximum specified time and then interrupts all the client
 * threads passed as the Vector at initialization of this thread.
 * 
 * The maximum execution time passed is assumed to be in seconds.
 * 
 * @author sudipto
 *
 */
public class TerminatorThread extends Thread {
  
  private Vector<Thread> threads;
  private Thread thread;
  private long maxExecutionTime;
  private Workload workload;
  private long waitTimeOutInMS;
  
  public TerminatorThread(long maxExecutionTime, Vector<Thread> threads, Workload workload) {
    this.maxExecutionTime = maxExecutionTime;
    this.threads = threads;
    this.workload = workload;
    waitTimeOutInMS = 2000;
    this.thread=null;
    System.err.println("Maximum execution time specified as: " + maxExecutionTime + " secs");
  }
  
  public TerminatorThread(long maxExecutionTime, Thread thread, Workload workload) {
    this.maxExecutionTime = maxExecutionTime;
    this.threads=null;
    this.thread=thread;
    this.workload = workload;
    waitTimeOutInMS = 2000;
    System.err.println("Maximum execution time specified as: " + maxExecutionTime + " secs");
  }
  
  public void run() {
    try {
      Thread.sleep(maxExecutionTime * 1000);
    } catch (InterruptedException e) {
      System.err.println("Could not wait until max specified time, TerminatorThread interrupted.");
      return;
    }
    System.err.println("Maximum time elapsed. Requesting stop for the workload.");
    workload.requestStop();
    System.err.println("Stop requested for workload. Now Joining!");
    if(threads!=null){
        for (Thread t : threads) {
          while (t.isAlive()) {
            try {
              t.join(waitTimeOutInMS);
              if (t.isAlive()) {
                System.err.println("Still waiting for thread " + t.getName() + " to complete. " +
                    "Workload status: " + workload.isStopRequested());
              }
            } catch (InterruptedException e) {
              // Do nothing. Don't know why I was interrupted.
            }
          }
        }
    }
    if(thread!= null){
        while (thread.isAlive()) {
            try {
              thread.join(waitTimeOutInMS);
              if (thread.isAlive()) {
                System.err.println("Still waiting for thread " + thread.getName() + " to complete. " +
                    "Workload status: " + workload.isStopRequested());
              }
            } catch (InterruptedException e) {
              // Do nothing. Don't know why I was interrupted.
            }
          }
    }
  }
}
