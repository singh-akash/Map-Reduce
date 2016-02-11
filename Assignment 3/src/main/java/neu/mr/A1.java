package neu.mr;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Vector;
import java.util.zip.GZIPInputStream;

import com.opencsv.CSVReader;

/**
 * @author Surekha Jadhwani
 * @author Akash Singh
 * 
 * Assignment A1 - analysis to find mean and median of flight ticket prices for airlines active 
 * in 2015.
 *
 */

public class A1 implements Runnable{

	ArrayList<String> input_files;
	private static Hashtable<String, CarrierRecord> record_container = new Hashtable<String, CarrierRecord>();
	
	private static Vector<Integer> good_records_count = new Vector<Integer>();
	private static Vector<Integer> bad_records_count = new Vector<Integer>();
	
	public A1(ArrayList<String> input_files)
	{
		this.input_files = input_files;
	}
	
	/**
	 * run method can be used by threaded applications to run
	 */
	public void run()
	{
		for (String input_file: this.input_files)
		{
			try 
			{	
				runner(input_file);
			}
			catch (FileNotFoundException e) 
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
			catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Method that takes input file as parameter and calculates mean of the carrier's 
	 * ticket prices
	 * @param input_file: the input file of data in gzip format
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	private synchronized void runner(String input_file) throws FileNotFoundException, IOException
	{
		
		GZIPInputStream gz_input_stream = new GZIPInputStream(new FileInputStream(input_file));
	    BufferedReader br = new BufferedReader(new InputStreamReader(gz_input_stream));
	    CSVReader r = new CSVReader(br);
	    
	    String[] record;
	    
	    while ((record = r.readNext()) != null)
	    {
	    	if (FlightUtils.isRecordSane(record) && FlightUtils.isValidAverageTicketPrice(record))
	    		{
	    			String carrier_code = record[FlightUtils.RecordIndex._08_CARRIER_ID];
	    			int month = Integer.parseInt(record[FlightUtils.RecordIndex._02_MONTH]);
		    		Double ticket_price = Double.parseDouble(record[FlightUtils.RecordIndex._109_AVG_TICKET_PRICE]);
		    		
		    		CarrierRecord carrier_record = record_container.get(carrier_code);
		    		if (carrier_record == null)
		    		{
		    			carrier_record = new CarrierRecord(carrier_code);
		    		}
		    		
		    		carrier_record.updateActiveCarrier(record[FlightUtils.RecordIndex._00_YEAR]);
		    		carrier_record.addTicketPrice(ticket_price, month - 1);
					record_container.put(carrier_code, carrier_record);		
	    		}
	    }
	  
	    r.close(); 
	    gz_input_stream.close();
	    br.close();
	}	
	
	// Getters
	/**
	 * 
	 * @return the record container
	 */
	public static Hashtable<String, CarrierRecord> getRecordContainer()
	{
		return record_container;
	}
	
	/**
	 * Gives the count of bad records
	 * @return the count of records not found as sane
	 */
	public static long getBadRecordsCount()
	{
		long rec_count = 0;
		for (int i: bad_records_count)
			rec_count += i;
		return rec_count;
	}
	
	/**
	 * Gives the count of sane records
	 * @return the count of sane records
	 */
	public static long getValidRecordsCount()
	{
		long rec_count = 0;
		for (int i: good_records_count)
			rec_count += i;
		return rec_count;
	}
}