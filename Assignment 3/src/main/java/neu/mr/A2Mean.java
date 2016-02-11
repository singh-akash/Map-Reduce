package neu.mr;

import java.io.IOException;
import java.text.DecimalFormat;
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
import com.opencsv.CSVParser;

/**
 * @author Surekha Jadhwani
 * @author Akash Singh
 * 
 *         Assignment A2 - analysis to find mean of flight ticket prices for airlines active in 2015 using Map Reduce.
 *
 */
public class A2Mean extends Configured implements Tool {
	
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
			System.exit(ToolRunner.run(new A2Mean(), args));
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
		
		// Creates multiple mapper instances - one for each file in the input directory
		MultipleInputs.addInputPath(job, new Path(input_dir), TextInputFormat.class, MeanFlightMapper.class);
		
		// sets the output directory to the one specified in input argument
		FileOutputFormat.setOutputPath(job, new Path(output_dir));
		
		job.setReducerClass(MeanFlightReducer.class);
		job.setNumReduceTasks(1);
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
	static class MeanFlightMapper extends Mapper<LongWritable, Text, Text, Text> 
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			// Splits the given line into fields
			String[] record = csv_parser.parseLine(value.toString());
			
			/*System.out.println(FlightUtils.isRecordSane(record));
			System.out.println(FlightUtils.isValidAverageTicketPrice(record));*/
			
			// Checks if the record is valid and it has avergae ticket price value
			if (FlightUtils.isRecordSane(record) && FlightUtils.isValidAverageTicketPrice(record)) 
			{
				String carrier_code = record[FlightUtils.RecordIndex._08_CARRIER_ID];
				Double ticket_price = Double.parseDouble(record[FlightUtils.RecordIndex._109_AVG_TICKET_PRICE]);
				boolean is_active = (FlightUtils.ACTIVE_YEAR.equals(record[FlightUtils.RecordIndex._00_YEAR]));
				
				//System.out.println(is_active);
				
				// Write carrier code as key and price, month, active indicator as value on the context for reducer
				context.write(new Text(carrier_code), 
								(new Text(ticket_price.toString() + 
									FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR +
									record[FlightUtils.RecordIndex._02_MONTH] + 
									FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR + 
									is_active)));
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
			double[] sum = new double[12];
			int[] count = new int[12];
			boolean active = false;
			DecimalFormat dm = new DecimalFormat("0.00");
			
			//Calculates separate mean ticket price for each month 
			for (Text v: values)
			{
				double price = Double.parseDouble(v.toString().split(FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR)[0]);
				int month = Integer.parseInt(v.toString().split(FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR)[1]);
				sum[month - 1] += price;
				count[month - 1]++;
				// System.out.println(active);
				if (!active)
				{
					active = FlightUtils.stringToBoolean(v.toString().split(FlightUtils.MAPPER_REDUCER_VALUES_SEPARATOR)[2]);
				}
			}
			
			//System.out.println(active);
			if (active)
			{
				for (int i = 0; i < 12; i++)
				{
					if (count[i] != 0)
					{
						double mean = sum[i] / count[i];
						//System.out.println(mean);
						context.write(new Text((i + 1) + " " + key + " " + dm.format(mean)),new Text(""));
					}	
				}
			}
		}
	}
}