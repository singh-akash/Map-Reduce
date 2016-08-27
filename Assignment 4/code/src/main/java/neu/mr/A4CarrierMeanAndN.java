package neu.mr;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.opencsv.CSVParser;

/**
 * @author Surekha Jadhwani
 * @author Akash Singh
 * 
 *         Assignment A4 - First Map Reduce set
 *
 */
public class A4CarrierMeanAndN extends Configured implements Tool {
	
	// Uses external library to parse CSV
	static CSVParser csv_parser = new CSVParser();

	/**
	 * This method invokes the run method for processing
	 * @param args : Command line arguments
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException 
	{
		try 
		{
			System.exit(ToolRunner.run(new A4CarrierMeanAndN(), args));
		} 
		catch (Exception e) 
		{
			System.err.println("Cannot run the program with given inputs. Please check the arguments");
			e.printStackTrace();
		}
	}

	/**
	 * This method creates an instance of Job for parallel processing using Map Reduce
	 * @param args: Input and Output directory
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException
	{
		Configuration conf = new Configuration();
		conf.set("mapred.textoutputformat.separator", FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR);
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "FlightMeanPrice");
		job.setJar("job.jar");
		
		String input_dir = args[0];
		String output_dir = args[1];
		
		if (input_dir != null && input_dir.startsWith(FlightUtils.Arguments.INPUT_DIR_PREFIX)) 
		{
			input_dir = input_dir.substring(FlightUtils.Arguments.INPUT_DIR_PREFIX.length());
		}
		
		if (output_dir != null && output_dir.startsWith(FlightUtils.Arguments.OUTPUT_DIR_PREFIX)) {
			output_dir = output_dir.substring(FlightUtils.Arguments.OUTPUT_DIR_PREFIX.length());
		}

		// getting all the files in the input directory
		FileInputFormat.setInputDirRecursive(job, true);
		// adding input files will be recursively processed
		FileInputFormat.addInputPath(job, new Path(input_dir));
		// sets the output directory to the one specified in input argument
		FileOutputFormat.setOutputPath(job, new Path(output_dir));
		
		job.setMapperClass(MeanFlightMapper.class);
		job.setReducerClass(MeanFlightReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return(job.waitForCompletion(true) ? 0 : 1);
	}

	/**
	 * 
	 * @author Surekha Jadhwani
	 * @maintainer Akash Singh
	 * 
	 *  This is a mapper class which validates the given line and writes the carrier data output for reducer if the
	 *  record is valid
	 *
	 */
	static class MeanFlightMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			// Splits the given line into fields
			String[] record = csv_parser.parseLine(value.toString());
			
			// Checks if the record is valid and it has avergae ticket price value
			if (FlightUtils.isRecordSane(record) && FlightUtils.isValidAverageTicketPrice(record)) 
			{
				String carrier_code = record[FlightUtils.RecordIndex._08_CARRIER_ID];
				Double ticket_price = Double.parseDouble(record[FlightUtils.RecordIndex._109_AVG_TICKET_PRICE]);
				
				// Write carrier code, year as key and elapsed time, ticket price, count as value on the context for reducer
				context.write(new Text(carrier_code +
										FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR +
										record[FlightUtils.RecordIndex._00_YEAR]), 
								(new Text(record[FlightUtils.RecordIndex._50_CRS_ELAPSED_TIME] +
										FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR +
										ticket_price.toString() + 
										FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR +
										1)));
			}
		}
	}

	/**
	 * 
	 * @author Surekha Jadhwani
	 * @maintainer Akash Singh
	 * 
	 * This is a reducer class which processes the given carrier data 
	 * and outputs the mean price of the carrier if that carrier is active in 2015
	 *
	 */
	static class MeanFlightReducer extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context)  
		throws IOException, InterruptedException 
		{

			DecimalFormat dm = new DecimalFormat("0.00");

			Hashtable<String, CarrierRecord> record_container = new Hashtable<String, CarrierRecord>();
			
			for (Text v: values)
			{
				double price = Double.parseDouble(v.toString().
						split(FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR)[MapperValue._01_TICKET_PRICE]);
				long count = Long.parseLong(v.toString().
						split(FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR)[MapperValue._02_COUNT]);
			
				String carrier_key = key.toString() + FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR + v.toString().
										split(FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR)[MapperValue._00_TIME];
				
				CarrierRecord curr_carrier = record_container.get(carrier_key);
				
				if (curr_carrier == null)
				{
					curr_carrier = new CarrierRecord(carrier_key);
				}
				
				curr_carrier.addTicketPrice(price);
				curr_carrier.updateTicketCount(count);
				record_container.put(carrier_key, curr_carrier);
			}
			
			for (CarrierRecord curr_carrier: record_container.values())
			{
				curr_carrier.computeMean();
				context.write(new Text(curr_carrier.getCarrier()), 
						new Text(dm.format(curr_carrier.getMeanTicketPrice())));
			}
		}
	}
	
	public static class MapperValue
	{
		public static final int _00_TIME 				= 0;
		public static final int _01_TICKET_PRICE	  	= 1;
		public static final int _02_COUNT 				= 2;
	}
}