package neu.mr;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;

import neu.mr.CarrierRecord;
/**
 * @author Akash Singh
 * @author Surekha Jadhwani
 * Class to test Assignment 1
 *
 */

public class Plain {
		
	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException 
	{	
		boolean run_in_parallel = false;
		
		String folder_argument = null;
		String query_type = null;
		
		if (args.length == 3)
		{
			run_in_parallel = FlightUtils.Arguments.OPTION_RUN_PARALLEL.equals(args[0]);
			folder_argument = args[1];
			query_type = args[2];
		}
		else if (args.length == 2)
		{
			folder_argument = args[0];
			query_type = args[1];
		}
		
		if (folder_argument != null &&
			folder_argument.startsWith(FlightUtils.Arguments.INPUT_DIR_PREFIX))
		{
			folder_argument = folder_argument.substring(FlightUtils.Arguments.INPUT_DIR_PREFIX.length());
		}
		
		if (query_type != null &&
			query_type.startsWith(FlightUtils.Arguments.QUERY_PREFIX))
		{
			query_type = query_type.substring(FlightUtils.Arguments.QUERY_PREFIX.length());
		}
		
		/*System.out.println(folder_argument);
		System.out.println(query_type);*/
		
		File folder = new File(folder_argument);
		File[] list_of_files = folder.listFiles();
		
		/*System.out.println(run_in_parallel);*/
		
		if (run_in_parallel)
		{
			Thread[] threads = new Thread[MAX_THREADS];
			ArrayList<ArrayList<String>> input_files = new ArrayList<ArrayList<String>>();
			
			int i = 0;
			int j = 0;
			
			for (i = 0; i < MAX_THREADS; i++)
			{
				input_files.add(new ArrayList<String>());
			}
			
			i = 0;
			while (i < list_of_files.length)
			{
				input_files.get(j).add(list_of_files[i].getAbsolutePath());
				i++;
				j = (j + 1) % MAX_THREADS; 
			}
			
			for (i = 0; i < threads.length; i++)
			{
				threads[i] = new Thread(new A1(input_files.get(i)));
				threads[i].start();
			}
	
			for (Thread t: threads)
			{
				t.join();
			}
		}
		else
		{
			ArrayList<String> input_files = new ArrayList<String>();
			
			for (File f: list_of_files)
			{
				input_files.add(f.getAbsolutePath());
			}
			new A1(input_files).run();
		}
				
		ArrayList<CarrierRecord> carrier_records = new ArrayList<CarrierRecord>(A1.getRecordContainer().values());
		
		/*System.out.println(query_type);*/
		
		for (CarrierRecord carrier_record : carrier_records)
		{
			if (carrier_record.getIsCarrierActive())
			{
				if (FlightUtils.Query.AVG_PRICE.equals(query_type))
					carrier_record.computeMean();
				else if (FlightUtils.Query.MEDIAN.equals(query_type))
					carrier_record.computeMedian();
				else if (FlightUtils.Query.FAST_MEDIAN.equals(query_type))
					carrier_record.computeFastMedian();
			}
		}
		    
		for (CarrierRecord carrier_record : carrier_records)
	    {
	    	if (carrier_record.getIsCarrierActive())
	    	{
	    		for(int i = 0; i < 12; i++)
	    		{
	    			if(FlightUtils.Query.AVG_PRICE.equals(query_type))  
	    			{
	    				double mean = carrier_record.getMeanTicketPrice(i); 
	    				if (mean != 0)
	    				{
	    					System.out.println((i + 1) + " " + carrier_record.getCarrier() + " " + String.format("%.2f", mean));
	    				}
	    			}
	    			else if (FlightUtils.Query.MEDIAN.equals(query_type) || FlightUtils.Query.FAST_MEDIAN.equals(query_type))
	    			{
	    				double median = carrier_record.getMedianTicketPrice(i);
	        			if (median != 0)
	        			{
	        				System.out.println((i + 1) + " " + carrier_record.getCarrier() + " " + String.format("%.2f", median));
	        			}
	    			}
	    		}
	    	}
	    }	
	}
	
	public static final int MAX_THREADS = 5;
}