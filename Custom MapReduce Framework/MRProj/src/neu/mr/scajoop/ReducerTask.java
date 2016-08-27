package neu.mr.scajoop;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;

import neu.mr.node.AbstractServerPersistent;
import neu.mr.node.Server;
import neu.mr.scajoop.io.Text;
import neu.mr.scajoop.mapreduce.Reducer;

/**
 * Task to sort the data, spawn reducers and write final output.
 * @author Surekha Jadhwani, Akash Singh
 */
public class ReducerTask {

	private static final Logger LOG = LogManager.getLogger(ReducerTask.class);
	private static final String CRLF = "\r\n";		
	private static final String EOF = "EOF";

	@SuppressWarnings (value="rawtypes")		
	private final Reducer reducer;
	private final int inputDataPortNum;
	private final String name;
	private final String outputFileDir;

	/**
	 * Constructor for ReducerTask
	 * @param reducerClassName: User defined reducer class name
	 * @param inputDataPortNum: Reducer data port number
	 * @param name: Reducer ID
	 * @param outputFileDir: Output directory
	 * @throws Exception
	 */
	@SuppressWarnings("rawtypes")
	public ReducerTask(String reducerClassName, int inputDataPortNum, String name, String outputFileDir) throws Exception {
		this.reducer = (Reducer) Class.forName(reducerClassName).newInstance();
		this.inputDataPortNum = inputDataPortNum;
		this.name = name;
		this.outputFileDir = outputFileDir;
	}

	/** Starts daemon thread to read input data from socket. 
	 * @throws InterruptedException */
	private void startDataThread() throws InterruptedException {
		Server reduceDataTask = new Server(inputDataPortNum, new ReducerStrategy(outputFileDir, name, reducer));
		Thread thread = new Thread(reduceDataTask);
		thread.setDaemon(true);
		thread.start();
		thread.join();
	}

	public static void main(String[] args) {
		try {
			if (args.length < 3) {
				System.out.println("Wrong # of command line parameters.");
				return;
			}

			ReducerTask reduceTask = new ReducerTask(args[0], Integer.parseInt(args[1]), args[2], args[3]);	
			// Start daemon thread to read input data from socket.
			reduceTask.startDataThread();

		} catch (Exception ex) {
			LOG.error("", ex);
		}
	}

	/**
	 * Strategy to processes input data from Partitioner.
	 * @author Surekha Jadhwani, Akash Singh
	 *
	 */
	private static class ReducerStrategy extends AbstractServerPersistent {
		private final String outputFileDir;
		private final String name;
		@SuppressWarnings("rawtypes")
		private final Reducer reducer;

		Map<String, ArrayList<Text>> kvpairs = new HashMap<String, ArrayList<Text>>(); 
		List<String> keys = new ArrayList<String>();

		@SuppressWarnings("rawtypes")
		public ReducerStrategy(String outputFileDir, String name, Reducer reducer) {
			this.outputFileDir = outputFileDir;
			this.name = name;
			this.reducer = reducer;
		}

		/** Echo name specific for this server. */
		@Override
		public String getTypeName() {
			return "Reducer Data";
		}

		FileWriter fw = null;

		// Implement Reducer Context to write final output to the file
		@SuppressWarnings("rawtypes")
		Context context = new Context() {
			@Override
			public synchronized void write(Object key, Object value) throws IOException {
				fw.write("" + key + "," + value + CRLF);
			}
		};

		/** Process input data. */
		@Override
		protected void handleInput(String input, Reader reader) throws IOException {
			try {
				if (EOF.equalsIgnoreCase(input)) {
					LOG.info("EOF received.");
					// Create temporary file for writing output
					File file = File.createTempFile("temporary/part-r-0000"+name,"");
					file.deleteOnExit();
					fw = new FileWriter(file);
					// Apply reduce function
					applyReduce();
					fw.close();
					writeOutput(file);
					return;
				}

				int index = input.indexOf(',');
				if (index == -1 ||
						index == input.length() - 1){
					LOG.info("Bad record: " + input);
					return;
				}

				String key = input.substring(0, index);
				String value = input.substring(index + 1);
				if (kvpairs.get(key) == null){
					kvpairs.put(key, new ArrayList<Text>());
					keys.add(key);
				}

				ArrayList<Text> currList = kvpairs.get(key);
				currList.add(new Text(value));
				kvpairs.put(key, currList);
			} catch (Exception ex) {
				LOG.error("Inside handleInput", ex);
				System.exit(0);
			}
		}

		/**
		 * Invoke user-defined reducer function for each key in multithreaded way  
		 */
		private void applyReduce(){
			int n = keys.size();

			int i = 0;
			while (i < n) {			
				int j = 0;
				ArrayList<Thread> threads = new ArrayList<Thread>();

				while (j < MAX_THREADS && i < n) {
					threads.add(new Thread(new ReduceRunner(reducer, 
							new Text(keys.get(i)),
							kvpairs.get(keys.get(i)), 
							context)));
					i++;
					j++;
				}

				//Execute the threads
				for (Thread t: threads)
					try{
						t.start();
					} 
				catch (Exception e) {
					System.out.println(t.getId());
				}

				for(Thread t: threads)
					try {
						t.join(); // wait for previous thread to finish
					} catch (InterruptedException e) {
						new Error(e);
					}
			}
		} 

		/**
		 * Invoke reduce function 
		 * @author Surekha Jadhwani, Akash Singh
		 *
		 */
		public class ReduceRunner implements Runnable{

			@SuppressWarnings("rawtypes")
			Reducer reducer;
			Text key;
			ArrayList<Text> values;
			@SuppressWarnings("rawtypes")
			Context context;

			@SuppressWarnings("rawtypes")
			public ReduceRunner(Reducer reducer, Text key, ArrayList<Text> values, Context context)
			{
				this.reducer = reducer;
				this.key = key;
				this.values = values;
				this.context = context;
			}

			@SuppressWarnings("unchecked")
			public void run() {				
				this.reducer.reduce(this.key, this.values, this.context);
			}
		}

		/**
		 * Copy temporary output file to given S3 or local directory
		 * @param file: Temporary output file
		 * @throws IOException
		 */
		private void writeOutput(File file) throws IOException {

			if(outputFileDir.startsWith("s3://"))
			{
				try
				{	
					String[] r = outputFileDir.split("/");
					String bucketName = r[2];  //Eg: inputDirName is split as s3:,  , cs6240sp16, climate
					String key = "";
					// Get path of all sub-directories after bucket name, if any
					if(r.length >= 3)
					{
						for(int i= 3; i< r.length; i++)
						{
							key = key.concat(r[i]).concat("/");
						}
					}
					AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
					AmazonS3 s3client = new AmazonS3Client(credentials);
					AccessControlList acl = new AccessControlList();
					acl.grantPermission(GroupGrantee.AllUsers, Permission.FullControl);
					s3client.putObject(new PutObjectRequest(bucketName, key+"part-r-0000"+name, file).withAccessControlList(acl));

				}
				catch(Exception ex)
				{
					LOG.error("Inside writeOutput", ex);
					return;
				}
			}
			else
			{
				file.renameTo(new File(outputFileDir+"/part-r-0000"+name));
			}

			LOG.info("Complete.");
			System.exit(0);		// temporary
		}

		private static final int MAX_THREADS = 6;
	}
}
