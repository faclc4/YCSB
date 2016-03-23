package com.yahoo.ycsb;

import com.yahoo.ycsb.measurements.ResultHandler;

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
