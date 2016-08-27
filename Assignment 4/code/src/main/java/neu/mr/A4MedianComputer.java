package neu.mr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.AmazonS3Client;
import com.opencsv.CSVParser;

import neu.mr.FlightUtils.RecordIndex;

/**
 * @author Surekha Jadhwani
 * @author Akash Singh
 * 
 *         Assignment A4 - First Map Reduce set
 *
 */
public class A4MedianComputer extends Configured implements Tool {
	
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
			System.exit(ToolRunner.run(new A4MedianComputer(), args));
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
		String r_input_file = args[2];
		int n;
		String N_string = args[3];
		if (r_input_file != null && r_input_file.startsWith(FlightUtils.Arguments.R_INPUT_PREFIX)) {
			r_input_file = r_input_file.substring(FlightUtils.Arguments.R_INPUT_PREFIX.length());
		}
		
		n = (N_string != null && N_string.startsWith(FlightUtils.Arguments.TIME_PREFIX)) ?
			Integer.parseInt(N_string.substring(FlightUtils.Arguments.TIME_PREFIX.length())) :
			Integer.parseInt(N_string);
	
		// Compute Least Expensive Carrier for all Years
	
		Hashtable<Integer, A4MinCarrier> min_carriers = new Hashtable<Integer, A4MinCarrier>();
			
		//BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(r_input_file))));
		
		AmazonS3 s3Client = new AmazonS3Client();        
		S3Object object = s3Client.getObject(new GetObjectRequest(args[4], "LM.csv"));
		
		InputStream objectData = object.getObjectContent();
		BufferedReader br = new BufferedReader(new InputStreamReader(objectData));
		// Process the objectData stream.
		br.readLine();
		String line;
		while ((line = br.readLine()) != null)
		{
			String slope_data[] = csv_parser.parseLine(line);
			String carrier = slope_data[FlightUtils.SlopeData._00_CARRIER];
			int year = Integer.parseInt(slope_data[FlightUtils.SlopeData._01_YEAR]);
			double intercept = Double.parseDouble(slope_data[FlightUtils.SlopeData._02_INTERCEPT]);
			double slope = Double.parseDouble(slope_data[FlightUtils.SlopeData._03_SLOPE]);
			double min_slope_intercept_val = slope * n + intercept;
			
			A4MinCarrier curr_min = min_carriers.get(year);
			
			if (curr_min == null ||
				curr_min.getMin_slope_intercept_val() > min_slope_intercept_val)
			{
				// System.out.println(curr_min.getMin_slope_intercept_val() + " " + min_slope_intercept_val);
				curr_min = new A4MinCarrier(carrier, min_slope_intercept_val);
				min_carriers.put(year, curr_min);
			}
		}
		
		br.close();
		objectData.close();
		// Finding max occurrences of carriers
		Hashtable<String, Integer> carrier_count = new Hashtable<String, Integer>();
		
		for (A4MinCarrier carrier : min_carriers.values())
		{
			int count = (carrier_count.get(carrier.getCarrier()) != null) ? 
						carrier_count.get(carrier.getCarrier()) : 1;
			carrier_count.put(carrier.getCarrier(), count + 1);
		}
		
		int max = 0;
		String result_carrier = null;
		for (String key : carrier_count.keySet())
		{
			if (carrier_count.get(key) > max)
			{
				max = carrier_count.get(key);
				result_carrier = key;
			}
		}
		
		// Printing the result
		//System.out.println(result_carrier);
		
		// Hadoop Map Reduce job
		Configuration conf = getConf();
		conf.set("mapred.textoutputformat.separator", FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR);
		conf.set(RESULT_CARRIER_PROP, result_carrier);
		@SuppressWarnings("deprecation")
		Job job = Job.getInstance(conf, "");
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
		
		// Creates multiple mapper instances - one for each file in the input directory
		MultipleInputs.addInputPath(job, new Path(input_dir), TextInputFormat.class, MedianComputerMapper.class);
		// sets the output directory to the one specified in input argument
		FileOutputFormat.setOutputPath(job, new Path(output_dir));
		
		job.setReducerClass(MedianComputerReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		return (job.waitForCompletion(true) ? 0 : 1);
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
	static class MedianComputerMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			// Splits the given line into fields
			String[] record = csv_parser.parseLine(value.toString());
			
			String result_carrier = context.getConfiguration().get(RESULT_CARRIER_PROP);
			
			// Checks if the record is valid and it has average ticket price value
			if (result_carrier.equals(record[RecordIndex._08_CARRIER_ID]) && 
				FlightUtils.isRecordSane(record) && 
				FlightUtils.isValidAverageTicketPrice(record)) 
			{
				String key_of_mapper = record[FlightUtils.RecordIndex._08_CARRIER_ID] +
										FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR +
										record[FlightUtils.RecordIndex._00_YEAR] +
										FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR +
										WeekUtils.getWeek(Integer.parseInt(record[FlightUtils.RecordIndex._02_MONTH]),
												Integer.parseInt(record[FlightUtils.RecordIndex._03_DAY_OF_MONTH]),
												Integer.parseInt(record[FlightUtils.RecordIndex._00_YEAR]));
		 
				Double ticket_price = Double.parseDouble(record[FlightUtils.RecordIndex._109_AVG_TICKET_PRICE]);
				
				// Writing carrier, year, week as key and price as value
				context.write(new Text(key_of_mapper), 
								(new Text(String.valueOf(ticket_price))));
			}
		}
	}

	/**
	 * 
	 * @author Surekha Jadhwani
	 * @maintainer Akash Singh
	 * 
	 * This is a reducer class which gives final output of weekly median prices
	 *
	 */
	static class MedianComputerReducer extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context)  
		throws IOException, InterruptedException 
		{

			DecimalFormat dm = new DecimalFormat("0.00");
			
			ArrayList<Double> prices = new ArrayList<Double>();
			for (Text v: values)
			{
				prices.add(Double.parseDouble(v.toString()));
			}
			Collections.sort(prices);
			
			double median = (prices.size() % 2 == 0) ? 
							((prices.get(prices.size() / 2 + 1) +
									prices.get(prices.size() / 2)) / 2) 
							: prices.get(prices.size() / 2);
			
			context.write(key, new Text(dm.format(median)));
		}
	}
	
	public static final String RESULT_CARRIER_PROP = "neu.mr.result_carrier";
}