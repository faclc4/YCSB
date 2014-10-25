package com.yahoo.ycsb;

import com.yahoo.ycsb.measurements.Measurements;
import java.text.DecimalFormat;

/**
 *
 * @author Fábio Coelho
 */
public class StatusThreadReplayer extends Thread{
    //This Thread will periodically output throughput results.
    
	String _label;
	boolean _standardstatus;
        OpCounter opcounter = null;
	
	/**
	 * The interval for reporting status.
	 */
	public static final long sleeptime=10000;

	public StatusThreadReplayer(OpCounter opcounter,String label, boolean standardstatus)
	{
            this.opcounter= opcounter;
            _label=label;
            _standardstatus=standardstatus;
	}

	/**
	 * Run and periodically report status.
	 */
	public void run()
	{
		long st=System.currentTimeMillis();

		long lasten=st;
		long lasttotalops=0;
		
		boolean alldone;

		do 
		{
			alldone=true;
                        this.opcounter.resetOpsCounter();

                        int totalops = opcounter.getOppDone();
                        
			long en=System.currentTimeMillis();

			long interval=en-st;
			//double throughput=1000.0*((double)totalops)/((double)interval);

			double curthroughput=1000.0*(((double)(totalops-lasttotalops))/((double)(en-lasten)));
			
			lasttotalops=totalops;
			lasten=en;
			
			DecimalFormat d = new DecimalFormat("#.##");
			
			if (totalops==0)
			{
				System.err.println(_label+" "+(interval/1000)+" sec: "+totalops+" operations; "+Measurements.getMeasurements().getSummary());
			}
			else
			{
				System.err.println(_label+" "+(interval/1000)+" sec: "+totalops+" operations; "+d.format(curthroughput)+" current ops/sec; "+Measurements.getMeasurements().getSummary());
			}

			if (_standardstatus)
			{
			if (totalops==0)
			{
				System.out.println(_label+" "+(interval/1000)+" sec: "+totalops+" operations; "+Measurements.getMeasurements().getSummary());
			}
			else
			{
				System.out.println(_label+" "+(interval/1000)+" sec: "+totalops+" operations; "+d.format(curthroughput)+" current ops/sec; "+Measurements.getMeasurements().getSummary());
			}
			}

			try
			{
				sleep(sleeptime);
			}
			catch (InterruptedException e)
			{
				//do nothing
			}

		}
		while (!alldone);
	}
    
}
