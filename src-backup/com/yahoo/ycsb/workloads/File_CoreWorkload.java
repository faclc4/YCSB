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

package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.ResultHandler;
import com.yahoo.ycsb.measurements.ResultStorage;
import redis.clients.jedis.Jedis;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The relative
 * proportion of different kinds of operations, and other properties of the workload, are controlled
 * by parameters specified at runtime.
 * <p/>
 * Properties to control the client:
 * <UL>
 * <LI><b>fieldcount</b>: the number of fields in a record (default: 10)
 * <LI><b>fieldlength</b>: the size of each field (default: 100)
 * <LI><b>readallfields</b>: should reads read all fields (true) or just one (false) (default: true)
 * <LI><b>writeallfields</b>: should updates and read/modify/writes update all fields (true) or just one (false) (default: false)
 * <LI><b>readproportion</b>: what proportion of operations should be reads (default: 0.95)
 * <LI><b>updateproportion</b>: what proportion of operations should be updates (default: 0.05)
 * <LI><b>insertproportion</b>: what proportion of operations should be inserts (default: 0)
 * <LI><b>scanproportion</b>: what proportion of operations should be scans (default: 0)
 * <LI><b>readmodifywriteproportion</b>: what proportion of operations should be read a record, modify it, write it back (default: 0)
 * <LI><b>requestdistribution</b>: what distribution should be used to select the records to operate on - uniform, zipfian, hotspot, or latest (default: uniform)
 * <LI><b>maxscanlength</b>: for scans, what is the maximum number of records to scan (default: 1000)
 * <LI><b>scanlengthdistribution</b>: for scans, what distribution should be used to choose the number of records to scan,
 * for each scan, between 1 and maxscanlength (default: uniform)
 * <LI><b>insertorder</b>: should records be inserted in order by key ("ordered"), or in hashed order ("hashed") (default: hashed)
 * </ul>
 */
public class File_CoreWorkload extends Workload {

    /**
     * The name of the database table to run queries against.
     */
    public static final String TABLENAME_PROPERTY = "table";

    /**
     * The default name of the database table to run queries against.
     */
    public static final String TABLENAME_PROPERTY_DEFAULT = "usertable";

    public static String table;


    /**
     * The name of the property for the number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY = "fieldcount";

    /**
     * Default number of fields in a record.
     */
    public static final String FIELD_COUNT_PROPERTY_DEFAULT = "10";

    int fieldcount;

    /**
     * The name of the property for the field length distribution. Options are "uniform", "zipfian" (favoring short records), "constant", and "histogram".
     * <p/>
     * If "uniform", "zipfian" or "constant", the maximum field length will be that specified by the fieldlength property.  If "histogram", then the
     * histogram will be read from the filename specified in the "fieldlengthhistogram" property.
     */
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";
    /**
     * The default field length distribution.
     */
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

    /**
     * The name of the property for the length of a field in bytes.
     */
    public static final String FIELD_LENGTH_PROPERTY = "fieldlength";
    /**
     * The default maximum length of a field in bytes.
     */
    public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";

    /**
     * The name of a property that specifies the filename containing the field length histogram (only used if fieldlengthdistribution is "histogram").
     */
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";
    /**
     * The default filename containing a field length histogram.
     */
    public static final String FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

    /**
     * Generator object that produces field lengths.  The value of this depends on the properties that start with "FIELD_LENGTH_".
     */
    IntegerGenerator fieldlengthgenerator;

    /**
     * The name of the property for deciding whether to read one field (false) or all fields (true) of a record.
     */
    public static final String READ_ALL_FIELDS_PROPERTY = "readallfields";

    /**
     * The default value for the readallfields property.
     */
    public static final String READ_ALL_FIELDS_PROPERTY_DEFAULT = "true";

    boolean readallfields;

    /**
     * The name of the property for deciding whether to write one field (false) or all fields (true) of a record.
     */
    public static final String WRITE_ALL_FIELDS_PROPERTY = "writeallfields";

    /**
     * The default value for the writeallfields property.
     */
    public static final String WRITE_ALL_FIELDS_PROPERTY_DEFAULT = "false";

    boolean writeallfields;


    /**
     * The name of the property for the proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY = "readproportion";

    /**
     * The default proportion of transactions that are reads.
     */
    public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";

    /**
     * The name of the property for the proportion of transactions that are updates.
     */
    public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";

    /**
     * The default proportion of transactions that are updates.
     */
    public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.05";

    /**
     * The name of the property for the proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";

    /**
     * The default proportion of transactions that are inserts.
     */
    public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the proportion of transactions that are scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY = "scanproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final String SCAN_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the proportion of transactions that are read-modify-write.
     */
    public static final String READMODIFYWRITE_PROPORTION_PROPERTY = "readmodifywriteproportion";

    /**
     * The default proportion of transactions that are scans.
     */
    public static final String READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT = "0.0";

    /**
     * The name of the property for the the distribution of requests across the keyspace. Options are "uniform", "zipfian" and "latest"
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";

    /**
     * The default distribution of requests across the keyspace
     */
    public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for the max scan length (number of records)
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY = "maxscanlength";

    /**
     * The default max scan length.
     */
    public static final String MAX_SCAN_LENGTH_PROPERTY_DEFAULT = "1000";

    /**
     * The name of the property for the scan length distribution. Options are "uniform" and "zipfian" (favoring short scans)
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY = "scanlengthdistribution";

    /**
     * The default max scan length.
     */
    public static final String SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    /**
     * The name of the property for the order to insert records. Options are "ordered" or "hashed"
     */
    public static final String INSERT_ORDER_PROPERTY = "insertorder";

    /**
     * Default insert order.
     */
    public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

    /**
     * Percentage data items that constitute the hot set.
     */
    public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";

    /**
     * Default value of the size of the hot set.
     */
    public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

    /**
     * Percentage operations that access the hot set.
     */
    public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";

    /**
     * Default value of the percentage operations accessing the hot set.
     */
    public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

    /**
     * The input file for keys
     */
    public static final String KEYS_FILE_PROPERTY = "keys_file";


    /**
     * The redis database address for key querying
     */
    public static final String REDIS_DATABASE_PROPERTY = "redis_database";

    /**
     * Querying mode for the files in the database.
     */
    public static final String USE_FILE_COLUMNS_PROPERTY = "use_file_columns";


    /**
     * Default querying mode.
     */
    public static final String USE_FILE_COLUMNS_DEFAULT_PROPERTY = "false";

    /**
     * Flag to decide if the transaction time-line is stored or not.
     */
    public static final String STORE_TRANSACTION_TIMELINES_PROPERTY = "store_transaction_timelines";

    /**
     * Default flag to decide if the transaction time-line is stored or not.
     */
    public static final String STORE_TRANSACTION_TIMELINES_DEFAULT_PROPERTY = "false";

    /**
     * Flag to decide if the transaction time-line is stored or not.
     */
    public static final String TRANSACTION_TIMELINES_FOLDER_PROPERTY = "transaction_timelines_folder";

    /**
     * The intervals between scans.
     */
    public static final String SCAN_DELAY_PROPERTY = "scan_intervals";


    /**
     * The number of scans to be executed.
     */
    public static final String SCANS_TO_EXECUTE_PROPERTY = "scans_executed";
    
    /**
     * The default interval between scans.
     */
    public static final String SCAN_DELAY_DEFAULT_PROPERTY = "0";

    public static int scan_delay;
    
    public static int number_of_scans;
   
    public static int executed_scans = 0;

    public static long last_scan;

    public static ReentrantLock scan_lock;

    public static boolean scan_in_process = false;

    public static boolean REDIS_INPUT = true;

    public static boolean FILE_INPUT = false;

    public static boolean KEY_INPUT_SOURCE = FILE_INPUT;

    public static ArrayList<String> files_keys;

    public static String redis_connection_info;

    boolean use_file_columns;

    boolean store_transaction_timelines;

    String transaction_timeline_folder;

    IntegerGenerator keysequence;

    DiscreteGenerator operationchooser;

    IntegerGenerator keychooser;

    Generator fieldchooser;

    CounterGenerator transactioninsertkeysequence;

    IntegerGenerator scanlength;

    boolean orderedinserts;

    int recordcount;

    protected static IntegerGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
        IntegerGenerator fieldlengthgenerator;
        String fieldlengthdistribution = p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
        int fieldlength = Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
        String fieldlengthhistogram = p.getProperty(FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY, FIELD_LENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
        if (fieldlengthdistribution.compareTo("constant") == 0) {
            fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
        } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
            fieldlengthgenerator = new UniformIntegerGenerator(1, fieldlength);
        } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
            fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
        } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
            try {
                fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
            } catch (IOException e) {
                throw new WorkloadException("Couldn't read field length histogram file: " + fieldlengthhistogram, e);
            }
        } else {
            throw new WorkloadException("Unknown field length distribution \"" + fieldlengthdistribution + "\"");
        }
        return fieldlengthgenerator;
    }

    /**
     * Initialize the scenario.
     * Called once, in the main client thread, before any operations are started.
     */
    public void init(Properties p) throws WorkloadException {
        table = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);

        fieldcount = Integer.parseInt(p.getProperty(FIELD_COUNT_PROPERTY, FIELD_COUNT_PROPERTY_DEFAULT));
        fieldlengthgenerator = File_CoreWorkload.getFieldLengthGenerator(p);

        double readproportion = Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
        double updateproportion = Double.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
        double insertproportion = Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
        double scanproportion = Double.parseDouble(p.getProperty(SCAN_PROPORTION_PROPERTY, SCAN_PROPORTION_PROPERTY_DEFAULT));
        double readmodifywriteproportion = Double.parseDouble(p.getProperty(READMODIFYWRITE_PROPORTION_PROPERTY, READMODIFYWRITE_PROPORTION_PROPERTY_DEFAULT));
        recordcount = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY));
        String requestdistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
        int maxscanlength = Integer.parseInt(p.getProperty(MAX_SCAN_LENGTH_PROPERTY, MAX_SCAN_LENGTH_PROPERTY_DEFAULT));
        String scanlengthdistrib = p.getProperty(SCAN_LENGTH_DISTRIBUTION_PROPERTY, SCAN_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);

        int insertstart = Integer.parseInt(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));

        readallfields = Boolean.parseBoolean(p.getProperty(READ_ALL_FIELDS_PROPERTY, READ_ALL_FIELDS_PROPERTY_DEFAULT));
        writeallfields = Boolean.parseBoolean(p.getProperty(WRITE_ALL_FIELDS_PROPERTY, WRITE_ALL_FIELDS_PROPERTY_DEFAULT));

        if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
            orderedinserts = false;
        } else if (requestdistrib.compareTo("exponential") == 0) {
            double percentile = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY,
                    ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
            double frac = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY,
                    ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
            keychooser = new ExponentialGenerator(percentile, recordcount * frac);
        } else {
            orderedinserts = true;
        }

        keysequence = new CounterGenerator(insertstart);
        operationchooser = new DiscreteGenerator();
        if (readproportion > 0) {
            operationchooser.addValue(readproportion, "READ");
        }

        if (updateproportion > 0) {
            operationchooser.addValue(updateproportion, "UPDATE");
        }

        if (insertproportion > 0) {
            operationchooser.addValue(insertproportion, "INSERT");
        }

        if (scanproportion > 0) {
            operationchooser.addValue(scanproportion, "SCAN");
        }

        if (readmodifywriteproportion > 0) {
            operationchooser.addValue(readmodifywriteproportion, "READMODIFYWRITE");
        }

        transactioninsertkeysequence = new CounterGenerator(recordcount);
        if (requestdistrib.compareTo("uniform") == 0) {
            keychooser = new UniformIntegerGenerator(0, recordcount - 1);
        } else if (requestdistrib.compareTo("zipfian") == 0) {
            //it does this by generating a random "next key" in part by taking the modulus over the number of keys
            //if the number of keys changes, this would shift the modulus, and we don't want that to change which keys are popular
            //so we'll actually construct the scrambled zipfian generator with a keyspace that is larger than exists at the beginning
            //of the test. that is, we'll predict the number of inserts, and tell the scrambled zipfian generator the number of existing keys
            //plus the number of predicted keys as the total keyspace. then, if the generator picks a key that hasn't been inserted yet, will
            //just ignore it and pick another key. this way, the size of the keyspace doesn't change from the perspective of the scrambled zipfian generator

            int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
            int expectednewkeys = (int) (((double) opcount) * insertproportion * 2.0); //2 is fudge factor

            keychooser = new ScrambledZipfianGenerator(recordcount + expectednewkeys);
        } else if (requestdistrib.compareTo("latest") == 0) {
            keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
        } else if (requestdistrib.equals("hotspot")) {
            double hotsetfraction = Double.parseDouble(p.getProperty(
                    HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
            double hotopnfraction = Double.parseDouble(p.getProperty(
                    HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
            keychooser = new HotspotIntegerGenerator(0, recordcount - 1,
                    hotsetfraction, hotopnfraction);
        } else {
            throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
        }

        fieldchooser = new UniformIntegerGenerator(0, fieldcount - 1);

        if (scanlengthdistrib.compareTo("uniform") == 0) {
            scanlength = new UniformIntegerGenerator(1, maxscanlength);
        } else if (scanlengthdistrib.compareTo("zipfian") == 0) {
            scanlength = new ZipfianGenerator(1, maxscanlength);
        } else {
            throw new WorkloadException("Distribution \"" + scanlengthdistrib + "\" not allowed for scan length");
        }

        use_file_columns = Boolean.parseBoolean(p.getProperty(USE_FILE_COLUMNS_PROPERTY, USE_FILE_COLUMNS_DEFAULT_PROPERTY));

        files_keys = new ArrayList<String>();

        String keys_file_path = p.getProperty(KEYS_FILE_PROPERTY);
        String redis_database_info = p.getProperty(REDIS_DATABASE_PROPERTY);

        if (keys_file_path == null && redis_database_info == null) {
            throw new WorkloadException("No input source for keys define a file with \"keys_file\" " +
                    "or a redis database with \"redis_database\" ");
        }

        if (keys_file_path != null) {

            FileInputStream input_file_stream;
            try {
                input_file_stream = new FileInputStream(keys_file_path);
            } catch (FileNotFoundException e) {
                throw new WorkloadException("Error when opening file for key retrieval: " + keys_file_path, e);
            }

            BufferedReader input_reader = new BufferedReader(new InputStreamReader(input_file_stream));

            try {
                String line = null;

                while ((line = input_reader.readLine()) != null) {
                    files_keys.add(line.trim());
                }
            } catch (Exception e) {
                throw new WorkloadException("Error when opening keys files", e);
            }

            try {
                input_file_stream.close();
            } catch (IOException e) {
                throw new WorkloadException("Error when closing file after call retrieval.", e);
            }
        }

        if (redis_database_info != null) {
            KEY_INPUT_SOURCE = REDIS_INPUT;
            redis_connection_info = redis_database_info;
            String[] connection_info = redis_connection_info.split(":");
            String host = connection_info[0];
            String port = connection_info[1];
            System.out.println("Redis database in "+host+" : "+Integer.parseInt(port));
        }

        store_transaction_timelines = Boolean.parseBoolean(p.getProperty(STORE_TRANSACTION_TIMELINES_PROPERTY, STORE_TRANSACTION_TIMELINES_DEFAULT_PROPERTY));

        if (store_transaction_timelines) {
            transaction_timeline_folder = p.getProperty(TRANSACTION_TIMELINES_FOLDER_PROPERTY);
            if (transaction_timeline_folder == null) {
                throw new WorkloadException("No output folder set for the transaction time-line storage. " +
                        "Add a \"transaction_timelines_folder\" tag with a valid and writable folder");
            }
            File timeline_folder = new File(transaction_timeline_folder);

            if (!timeline_folder.exists()) {
                boolean created = timeline_folder.mkdir();
                if (!created) {
                    throw new WorkloadException("The output folder for the transaction time-line storage couldn't be created. Check if it is in a valid path");
                }
            }

            if (!timeline_folder.isDirectory()) {
                System.out.println("The output folder for the transaction time-line storage is a file. Using enclosing folder");
                timeline_folder = timeline_folder.getParentFile();
            }

            if (!timeline_folder.canWrite()) {
                throw new WorkloadException("The output folder for the transaction time-line storage is not writable. Check if it is in a valid path");
            }

            ResultStorage.configure("YCSB", timeline_folder.getAbsolutePath(), ResultStorage.AGGREGATED_RESULTS);
        }

        scan_lock = new ReentrantLock();
        
        
        scan_delay = Integer.parseInt(p.getProperty(SCAN_DELAY_PROPERTY,SCAN_DELAY_DEFAULT_PROPERTY));
        number_of_scans = Integer.parseInt(p.getProperty(SCANS_TO_EXECUTE_PROPERTY,"-1"));

        last_scan = System.currentTimeMillis();
        
    }


    @Override
    public Object initThread(Properties p, int mythreadid, int threadcount) throws WorkloadException {

        ResultHandler resultHandler = null;
        if (store_transaction_timelines) {
            resultHandler = ResultStorage.getQueryResultHandlerInstance(Integer.toString(mythreadid));
        }

        Jedis redis_client = null;

        if(KEY_INPUT_SOURCE == REDIS_INPUT){
            String[] connection_info = redis_connection_info.split(":");
            String host = connection_info[0];
            String port = connection_info[1];
            redis_client = new Jedis(host,Integer.parseInt(port));
        }

        ClientThreadState clientThreadState = new ClientThreadState(redis_client,resultHandler);
             
     //   Pair<ResultHandler,Jedis> thread_state = new Pair<ResultHandler, Jedis>(resultHandler,redis_client);
        return clientThreadState;
    }

    @Override
    public void cleanup() throws WorkloadException {
        if (store_transaction_timelines) {
            ResultStorage.collect_and_print();
        }
    }

    public String buildKeyName(long keynum) {

        if (!orderedinserts)
        {
            keynum=Utils.hash(keynum);
        }
        return "user"+keynum;

    }

    public String fetchKeyName(long keynum,Object thread_state) {

        ClientThreadState state = (ClientThreadState) thread_state;
        Jedis redis_client  = state.getRedis_connection();

        String key = "";

        if(KEY_INPUT_SOURCE==REDIS_INPUT){

            key = redis_client.get(keynum+"");

            if(key ==null){
                System.out.println("Null on key: "+Long.toString(keynum));
            }

        }else{
            key = files_keys.get((int) keynum);
        }

        return key;

    }
    
    
    
    HashMap<String, ByteIterator> buildValues() {
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();

        for (int i = 0; i < fieldcount; i++) {
            String fieldkey = "field" + i;
            ByteIterator data = new RandomByteIterator(fieldlengthgenerator.nextInt());
            values.put(fieldkey, data);
        }
        return values;
    }

    HashMap<String, ByteIterator> buildUpdate() {
        //update a random field
        HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
        String fieldname = "field" + fieldchooser.nextString();
        ByteIterator data = new RandomByteIterator(fieldlengthgenerator.nextInt());
        values.put(fieldname, data);
        return values;
    }

    /**
     * Do one insert operation. Because it will be called concurrently from multiple client threads, this
     * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
     * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
     * effects other than DB operations.
     */
    public boolean doInsert(DB db, Object threadstate) {
        int keynum = keysequence.nextInt();
        String dbkey = buildKeyName(keynum);
        HashMap<String, ByteIterator> values = buildValues();
        if (db.insert(table, dbkey, values) == 0)
            return true;
        else
            return false;
    }

    /**
     * Do one transaction operation. Because it will be called concurrently from multiple client threads, this
     * function must be thread safe. However, avoid synchronized, or the threads will block waiting for each
     * other, and it will be difficult to reach the target throughput. Ideally, this function would have no side
     * effects other than DB operations.
     */
    public boolean doTransaction(DB db, Object threadstate) {

        ClientThreadState state = (ClientThreadState) threadstate;
        ResultHandler resultHandler = state.getClient_resultHandler();
        
        String op = operationchooser.nextString();

        long init_transaction_time = System.currentTimeMillis();

        if (op.compareTo("READ") == 0) {
            if(state.isScan_thread()){    //scan thread only
                return true;
            }
            doTransactionRead(db,threadstate);
        } else if (op.compareTo("UPDATE") == 0) {
            doTransactionUpdate(db);
        } else if (op.compareTo("INSERT") == 0) {
            doTransactionInsert(db);
        } else if (op.compareTo("SCAN") == 0) {
            
            boolean do_scan =  false;
            scan_lock.lock();

                long current_time = System.currentTimeMillis();
                if(!scan_in_process && ((current_time-last_scan)/1000) >= scan_delay
                        && (number_of_scans == -1 || executed_scans < number_of_scans) ){
                    scan_in_process = true;
                    do_scan = true;
                }
            scan_lock.unlock();

            if(do_scan){
                doTransactionScan(db);
                scan_lock.lock();
                scan_in_process = false;
                executed_scans ++;
                state.setScan_thread(true);
                last_scan = System.currentTimeMillis();
                scan_lock.unlock();
            }
            else{
                op = "READ";
                if(state.isScan_thread()){    //scan thread only
                    return true;
                }else{
                    doTransactionRead(db,threadstate);
                }
            }
        } else {
            doTransactionReadModifyWrite(db);
        }

        long end_transaction_time = System.currentTimeMillis();

        if (store_transaction_timelines) {
            resultHandler.recordTimeline(op, init_transaction_time, end_transaction_time);
        }

        return true;
    }


    int nextKeynum() {
        int keynum;
        if (keychooser instanceof ExponentialGenerator) {
            do {
                keynum = transactioninsertkeysequence.lastInt() - keychooser.nextInt();
            }
            while (keynum < 0);
        } else {
            do {
                keynum = keychooser.nextInt();
            }
            while (keynum > transactioninsertkeysequence.lastInt());
        }
        return keynum;
    }

    public void doTransactionRead(DB db,Object thread_state) {
        //choose a random key
        int keynum = nextKeynum();

        String keyname = fetchKeyName(keynum,thread_state);

        HashSet<String> fields = null;

        if (use_file_columns) {
            String[] folders_and_files = keyname.split("/");

            String fieldname = folders_and_files[folders_and_files.length - 1];
            keyname = keyname.replace("/" + fieldname, "");

            fields = new HashSet<String>();
            fields.add(fieldname);
        }

        db.read(table, keyname, fields, new HashMap<String, ByteIterator>());
    }

    public void doTransactionReadModifyWrite(DB db) {
        //choose a random key
        int keynum = nextKeynum();

        String keyname = buildKeyName(keynum);

        HashSet<String> fields = null;

        if (!readallfields) {
            //read a random field
            String fieldname = "field" + fieldchooser.nextString();

            fields = new HashSet<String>();
            fields.add(fieldname);
        }

        HashMap<String, ByteIterator> values;

        if (writeallfields) {
            //new data for all the fields
            values = buildValues();
        } else {
            //update a random field
            values = buildUpdate();
        }

        //do the transaction

        long st = System.currentTimeMillis();

        db.read(table, keyname, fields, new HashMap<String, ByteIterator>());

        db.update(table, keyname, values);

        long en = System.currentTimeMillis();

        Measurements.getMeasurements().measure("READ-MODIFY-WRITE", (int) (en - st));
    }

    public void doTransactionScan(DB db) {
        //CHANGE: do a total scan

        //choose a random key
        //int keynum = nextKeynum();

        String startkeyname = "";//buildKeyName(keynum);

        //choose a random scan length
        //int len = scanlength.nextInt();
        int len = recordcount;

        HashSet<String> fields = null;

        if (!readallfields) {
            //read a random field
            String fieldname = "field" + fieldchooser.nextString();

            fields = new HashSet<String>();
            fields.add(fieldname);
        }

        db.scan(table, startkeyname, len, fields, new Vector<HashMap<String, ByteIterator>>());
    }

    public void doTransactionUpdate(DB db) {
        //choose a random key
        int keynum = nextKeynum();

        String keyname = buildKeyName(keynum);

        HashMap<String, ByteIterator> values;

        if (writeallfields) {
            //new data for all the fields
            values = buildValues();
        } else {
            //update a random field
            values = buildUpdate();
        }

        db.update(table, keyname, values);
    }

    public void doTransactionInsert(DB db) {
        //choose the next key
        int keynum = transactioninsertkeysequence.nextInt();

        String dbkey = buildKeyName(keynum);

        HashMap<String, ByteIterator> values = buildValues();
        db.insert(table, dbkey, values);
    }
}

class ClientThreadState{

    Jedis redis_connection;
    ResultHandler client_resultHandler;
    boolean scan_thread;

    ClientThreadState(Jedis redis_connection, ResultHandler client_resultHandler) {
        this.redis_connection = redis_connection;
        this.client_resultHandler = client_resultHandler;
        this.scan_thread = false;
    }

    public void setScan_thread(boolean scan_thread) {
        this.scan_thread = scan_thread;
    }

    public Jedis getRedis_connection() {
        return redis_connection;
    }

    public ResultHandler getClient_resultHandler() {
        return client_resultHandler;
    }

    public boolean isScan_thread() {
        return scan_thread;
    }
}