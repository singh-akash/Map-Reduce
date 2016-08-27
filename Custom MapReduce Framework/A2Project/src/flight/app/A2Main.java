package flight.app;

import java.io.IOException;

import flight.Flight;
import flight.InputFlightParser;
import neu.mr.scajoop.Context;
import neu.mr.scajoop.conf.Configuration;
import neu.mr.scajoop.conf.Configured;
import neu.mr.scajoop.fs.Path;
import neu.mr.scajoop.io.Text;
import neu.mr.scajoop.lib.input.FileInputFormat;
import neu.mr.scajoop.lib.output.FileOutputFormat;
import neu.mr.scajoop.mapreduce.Job;
import neu.mr.scajoop.mapreduce.Mapper;
import neu.mr.scajoop.mapreduce.Reducer;
import neu.mr.scajoop.util.Tool;
import neu.mr.scajoop.util.ToolRunner;

/**
 * Main MR class. Contains both the Mapper and the Reducer.
 * @author Joseph Sackett, Chris Willig, Surekha Jadhwani
 */
public class A2Main extends Configured implements Tool {
	
	/**
	 * This method creates an instance of Job for parallel processing using SCAJOOP Map Reduce framework
	 * @param args: Input and Output directory
	 * 
	 */
	public void run(String[] args) {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "AirlineMean");
		conf = job.getConfiguration();
		job.setJar("A2Proj.jar");
		
		job.setMapperClass(AirlineMapper.class);
		job.setReducerClass(AirlineReducer.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
	
	/**
	 * This method invokes the run method for processing
	 * @param args : Command line arguments 
	 * 
	 */
    public static void main(String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\ninput-dir output-dir");
        }
        try {
            ToolRunner.run(new A2Main(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
	/**
	 * @author Surekha Jadhwani
	 * This is a mapper class which validates the given line and writes the carrier 
	 * data output for reducer if the record is valid
	 *
	 */
	public static class AirlineMapper extends Mapper<Object, Text, Text, Text> {
	
		@Override
		public void map(Object key, Text line, Context<Text, Text> context) {
			InputFlightParser parser = new InputFlightParser();
			Flight flight = parser.buildValidFlight(line.toString());
			if (flight == null) {
				return;
			}
			
			Text carrier = new Text(flight.getCarrier());
			Text value = new Text("" + flight.getYear() + ',' + flight.getMonth() + ',' + flight.getPrice());

			try {
				context.write(carrier, value);
			} catch (IOException e) {
				throw new Error(e);				
			}
		}
	}
	
	/** 
	 * @author Surekha Jadhwani
	 * @maintainer Akash Singh
	 * This is a reducer class which processes the given carrier data 
	 * and outputs the mean price of the carrier for each month and active indicator
	 *
	 */
	public static class AirlineReducer extends Reducer<Text, Text, Text, Text> {

	    @SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) {
			int flightCount = 0;
			boolean active2015 = false;

			int[] priceCount = new int[12];
			double[] priceTotal = new double[12];

			for(Text value : values) {
				flightCount++;
				String[] fields = value.toString().split(",");
				int year = Integer.parseInt(fields[0]);
				int month = Integer.parseInt(fields[1]);
				int price = Integer.parseInt(fields[2]);

				if (!active2015) {
					active2015 = year == 2015;
				}
				priceCount[month - 1]++;
				priceTotal[month - 1] += price;
			}

			String out = "";
			for (int ix=0 ; ix<12 ; ix++) {
				out += String.format("%.2f,", ((float) priceTotal[ix] / (float) priceCount[ix]) / 100.0f);
			}
			out += (active2015 ? "1," : "0,");
			out += flightCount;

			try {
				context.write(key, new Text(out));
			} catch (IOException e) {
				throw new Error(e);
			}
		}
	}	
}
