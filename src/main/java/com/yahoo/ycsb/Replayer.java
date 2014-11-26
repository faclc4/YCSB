package com.yahoo.ycsb;

import com.yahoo.ycsb.measurements.Measurements;
import com.yahoo.ycsb.measurements.exporter.MeasurementsExporter;
import com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author Fábio Coelho
 * 
 * Run args: -db com.yahoo.ycsb.InfinispanGlue -load -db_path /home/fabio/Documents/Replayer/YCSB_Replay/dbs -P file_workloads/workload_1
 * 
 */
public class Replayer {
    
        public static final String OPERATION_COUNT_PROPERTY="operationcount";

	public static final String RECORD_COUNT_PROPERTY="recordcount";

	public static final String WORKLOAD_PROPERTY="workload";
	
	/**
	 * Indicates how many inserts to do, if less than recordcount. Useful for partitioning
	 * the load among multiple servers, if the client is the bottleneck. Additionally, workloads
	 * should support the "insertstart" property, which tells them which record to start at.
	 */
	public static final String INSERT_COUNT_PROPERTY="insertcount";
	
	/**
   * The maximum amount of time (in seconds) for which the benchmark will be run.
   */
  public static final String MAX_EXECUTION_TIME = "maxexecutiontime";

	public static void usageMessage()
	{
		System.out.println("Usage: java com.yahoo.ycsb.Client [options]");
		System.out.println("Options:");
		System.out.println("  -threads n: execute using n threads (default: 1) - can also be specified as the \n" +
				"              \"threadcount\" property using -p");
		System.out.println("  -target n: attempt to do n operations per second (default: unlimited) - can also\n" +
				"             be specified as the \"target\" property using -p");
		System.out.println("  -load:  run the loading phase of the workload");
                System.out.println("  -replay: run YCSB in REPLAY mode ");
                System.out.println("  -speedup : Should be used together with -replay tag. When used, inserts in replay mode are twice as fast ");
		System.out.println("  -t:  run the transactions phase of the workload (default)");
		System.out.println("  -db dbname: specify the name of the DB to use (default: com.yahoo.ycsb.BasicDB) - \n" +
				"              can also be specified as the \"db\" property using -p");
		System.out.println("  -P propertyfile: load properties from the given file. Multiple files can");
		System.out.println("                   be specified, and will be processed in the order specified");
		System.out.println("  -p name=value:  specify a property to be passed to the DB and workloads;");
		System.out.println("                  multiple properties can be specified, and override any");
		System.out.println("                  values in the propertyfile");
		System.out.println("  -s:  show status during run (default: no status)");
		System.out.println("  -l label:  use label for status (e.g. to label one experiment out of a whole batch)");
		System.out.println("");
		System.out.println("Required properties:");
		System.out.println("  "+WORKLOAD_PROPERTY+": the name of the workload class to use (e.g. com.yahoo.ycsb.workloads.CoreWorkload)");
		System.out.println("");
		System.out.println("To run the transaction phase from multiple servers, start a separate client on each.");
		System.out.println("To run the load phase from multiple servers, start a separate client on each; additionally,");
		System.out.println("use the \"insertcount\" and \"insertstart\" properties to divide up the records to be inserted");
	}

	public static boolean checkRequiredProperties(Properties props)
	{
		if (props.getProperty(WORKLOAD_PROPERTY)==null)
		{
			System.out.println("Missing property: "+WORKLOAD_PROPERTY);
			return false;
		}

		return true;
	}


	/**
	 * Exports the measurements to either sysout or a file using the exporter
	 * loaded from conf.
	 * @throws IOException Either failed to write to output stream or failed to close it.
	 */
	private static void exportMeasurements(Properties props, int opcount, long runtime)
			throws IOException
	{
		MeasurementsExporter exporter = null;
		try
		{
			// if no destination file is provided the results will be written to stdout
			OutputStream out;
			String exportFile = props.getProperty("exportfile");
			if (exportFile == null)
			{
				out = System.out;
			} else
			{
				out = new FileOutputStream(exportFile);
			}

			// if no exporter is provided the default text one will be used
			String exporterStr = props.getProperty("exporter", "com.yahoo.ycsb.measurements.exporter.TextMeasurementsExporter");
			try
			{
				exporter = (MeasurementsExporter) Class.forName(exporterStr).getConstructor(OutputStream.class).newInstance(out);
			} catch (Exception e)
			{
				System.err.println("Could not find exporter " + exporterStr
						+ ", will use default text reporter.");
				e.printStackTrace();
				exporter = new TextMeasurementsExporter(out);
			}

			exporter.write("OVERALL", "RunTime(ms)", runtime);
			double throughput = 1000.0 * ((double) opcount) / ((double) runtime);
			exporter.write("OVERALL", "Throughput(ops/sec)", throughput);

			Measurements.getMeasurements().exportMeasurements(exporter);
		} finally
		{
			if (exporter != null)
			{
				exporter.close();
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws InterruptedException
	{
		String dbname;
                String db_path = null;
		Properties props=new Properties();
		Properties fileprops=new Properties();
		boolean dotransactions=true;
                boolean replay=false;
		int threadcount=1;
		int target=0;
		boolean status=false;
		String label="";
                
                OpCounter opcounter = new OpCounter();
                
                //starts Thread_pool
                ExecutorService thread_pool = Executors.newCachedThreadPool();

		//parse arguments
		int argindex=0;

		if (args.length==0)
		{
			usageMessage();
			System.exit(0);
		}

		while (args[argindex].startsWith("-"))
		{
                        if (args[argindex].compareTo("-db_path")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
                                db_path = args[argindex];
				argindex++;
			}
			if (args[argindex].compareTo("-threads")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				int tcount=Integer.parseInt(args[argindex]);
				props.setProperty("threadcount", tcount+"");
				argindex++;
			}
			else if (args[argindex].compareTo("-target")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				int ttarget=Integer.parseInt(args[argindex]);
				props.setProperty("target", ttarget+"");
				argindex++;
			}
			else if (args[argindex].compareTo("-load")==0)
			{
				dotransactions=false;
				argindex++;
			}
			else if (args[argindex].compareTo("-t")==0)
			{
				dotransactions=true;
				argindex++;
			}
			else if (args[argindex].compareTo("-s")==0)
			{
				status=true;
				argindex++;
			}
                        else if (args[argindex].compareTo("-replay")==0)
			{
				replay=true;
				argindex++;
			}
			else if (args[argindex].compareTo("-db")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				props.setProperty("db",args[argindex]);
				argindex++;
			}
			else if (args[argindex].compareTo("-l")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				label=args[argindex];
				argindex++;
			}
			else if (args[argindex].compareTo("-P")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				String propfile=args[argindex];
				argindex++;

				Properties myfileprops=new Properties();
				try
				{
					myfileprops.load(new FileInputStream(propfile));
				}
				catch (IOException e)
				{
					System.out.println(e.getMessage());
					System.exit(0);
				}

				//Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
				for (Enumeration e=myfileprops.propertyNames(); e.hasMoreElements(); )
				{
				   String prop=(String)e.nextElement();
				   
				   fileprops.setProperty(prop,myfileprops.getProperty(prop));
				}

			}
			else if (args[argindex].compareTo("-p")==0)
			{
				argindex++;
				if (argindex>=args.length)
				{
					usageMessage();
					System.exit(0);
				}
				int eq=args[argindex].indexOf('=');
				if (eq<0)
				{
					usageMessage();
					System.exit(0);
				}

				String name=args[argindex].substring(0,eq);
				String value=args[argindex].substring(eq+1);
				props.put(name,value);
				//System.out.println("["+name+"]=["+value+"]");
				argindex++;
			}
			else
			{
				System.out.println("Unknown option "+args[argindex]);
				usageMessage();
				System.exit(0);
			}
			
			if(dotransactions){
                	   props.put("load", "false");
                	}
                	if(!dotransactions){
                    	   props.put("load", "true");
                	}

			if (argindex>=args.length)
			{
				break;
			}
		}

		if (argindex!=args.length)
		{
			usageMessage();
			System.exit(0);
		}

		//set up logging
		//BasicConfigurator.configure();

		//overwrite file properties with properties from the command line

		//Issue #5 - remove call to stringPropertyNames to make compilable under Java 1.5
		for (Enumeration e=props.propertyNames(); e.hasMoreElements(); )
		{
		   String prop=(String)e.nextElement();
		   
		   fileprops.setProperty(prop,props.getProperty(prop));
		}

		props=fileprops;

		if (!checkRequiredProperties(props))
		{
			System.exit(0);
		}
		
		long maxExecutionTime = Integer.parseInt(props.getProperty(MAX_EXECUTION_TIME, "0"));

		//get number of threads, target and db
		threadcount=Integer.parseInt(props.getProperty("threadcount","1"));
		dbname=props.getProperty("db","com.yahoo.ycsb.BasicDB");
		target=Integer.parseInt(props.getProperty("target","0"));
		
		//compute the target throughput
		double targetperthreadperms=-1;
		if (target>0)
		{
			double targetperthread=((double)target)/((double)threadcount);
			targetperthreadperms=targetperthread/1000.0;
		}	 

		System.out.println("YCSB Client 0.1");
		System.out.print("Command line:");
		for (int i=0; i<args.length; i++)
		{
			System.out.print(" "+args[i]);
		}
		System.out.println();
		System.err.println("Loading workload...");

		//set up measurements
		Measurements.setProperties(props);
		
		//load the workload
		ClassLoader classLoader = Client.class.getClassLoader();

		Workload workload=null;

		try 
		{
			Class workloadclass = classLoader.loadClass(props.getProperty(WORKLOAD_PROPERTY));

			workload=(Workload)workloadclass.newInstance();
		}
		catch (Exception e) 
		{  
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}

		try
		{
			workload.init(props);
		}
		catch (WorkloadException e)
		{
			e.printStackTrace();
			e.printStackTrace(System.out);
			System.exit(0);
		}
		
		//run the workload

		System.err.println("Starting test.");

		int opcount;
		if (!dotransactions){
                    //Loads data;
                        opcount=Integer.parseInt(props.getProperty(OPERATION_COUNT_PROPERTY,"0"));
                        //**************************************************************
                        //         START LOADER THREAD
                        //**************************************************************
                        DB db=null;
                        try
                        {
                                db=DBFactory.newDB(dbname,props);
                        }
                        catch (UnknownDBException e)
                        {
                                System.out.println("Unknown DB "+dbname);
                                System.exit(0);
                        }
                        //Initiates the Loader Thread.
                        Thread loader_thread = new Thread(new LoaderThread(db_path,opcounter,db,workload,thread_pool,props));
                        loader_thread.start();
                        loader_thread.join();
                        
                        //**************************************************************
                        //         START STATUS THREAD
                        //**************************************************************

                        StatusThreadReplayer statusthread=null;

                        if (status)
                        {
                                boolean standardstatus=false;
                                if (props.getProperty("measurementtype","").compareTo("timeseries")==0) 
                                {
                                        standardstatus=true;
                                }	

                                //Initiates the Status Thread.s
                                statusthread = new StatusThreadReplayer(opcounter,label,standardstatus);
                                statusthread.start();
                        }
                        long st=System.currentTimeMillis();

                            Thread terminator = null;
                            if (maxExecutionTime > 0) {
                              terminator = new TerminatorThread(maxExecutionTime, loader_thread, workload);
                              terminator.start();
                            }
                            int opsDone = 0;
                                        try{
                                                loader_thread.join();
                                                opsDone += opcounter.getOppDone();
                                        }
                                        catch (InterruptedException e){
                                        }
                                        long en=System.currentTimeMillis();

                                        if (terminator != null && !terminator.isInterrupted()) {
                              terminator.interrupt();
                            }

                                        if (status){
                                                statusthread.interrupt();
                                        }
                                        try{
                                                workload.cleanup();
                                        }
                                        catch (WorkloadException e){
                                                e.printStackTrace();
                                                e.printStackTrace(System.out);
                                                System.exit(0);
                                        }
                                        try{
                                                exportMeasurements(props, opsDone, en - st);
                                        } catch (IOException e){
                                                System.err.println("Could not export measurements, error: " + e.getMessage());
                                                e.printStackTrace();
                                                System.exit(-1);
                                        }
                                        System.exit(0);
           
		}
		else
		{// performs replay transactions                    
                //**************************************************************
                //         START DISPATCHER THREAD
                //**************************************************************
                
                    DB db=null;
                    try
                    {
                            db=DBFactory.newDB(dbname,props);
                    }
                    catch (UnknownDBException e)
                    {
                            System.out.println("Unknown DB "+dbname);
                            System.exit(0);
                    }
                    //Initiates the Dispatcher Thread.
                    Thread dispatcher_thread = new Thread(new DispatcherThread(db_path,opcounter,db,workload,thread_pool,props));
                    dispatcher_thread.start();
                    //dispatcher_thread.join();
                    
                //**************************************************************
                //         START STATUS THREAD
                //**************************************************************
                    
                    StatusThreadReplayer statusthread=null;

                    if (status)
                    {
                            boolean standardstatus=false;
                            if (props.getProperty("measurementtype","").compareTo("timeseries")==0) 
                            {
                                    standardstatus=true;
                            }	

                            //Initiates the Status Thread.s
                            statusthread = new StatusThreadReplayer(opcounter,label,standardstatus);
                            statusthread.start();
                    }

                    long st=System.currentTimeMillis();
                    Thread terminator = null;

                    if (maxExecutionTime > 0) {
                      terminator = new TerminatorThread(maxExecutionTime, dispatcher_thread, workload);
                      terminator.start();
                    }
                    int opsDone = 0;
                                try{
                                        dispatcher_thread.join();
                                        opsDone += opcounter.getOppDone();
                                }
                                catch (InterruptedException e){
                                }
                                long en=System.currentTimeMillis();

                                if (terminator != null && !terminator.isInterrupted()) {
                      terminator.interrupt();
                    }

                                if (status){
                                        statusthread.interrupt();
                                }
                                try{
                                        workload.cleanup();
                                }
                                catch (WorkloadException e){
                                        e.printStackTrace();
                                        e.printStackTrace(System.out);
                                        System.exit(0);
                                }
                                try{
                                        exportMeasurements(props, opsDone, en - st);
                                } catch (IOException e){
                                        System.err.println("Could not export measurements, error: " + e.getMessage());
                                        e.printStackTrace();
                                        System.exit(-1);
                                }
                                System.exit(0);
	}  
    }
        
           
}
