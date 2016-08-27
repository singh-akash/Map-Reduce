package neu.mr.scajoop;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import neu.mr.scajoop.io.Text;
import neu.mr.scajoop.mapreduce.Mapper;

/**
 * Task to process input data and spawn Mappers.
 * @author Surekha Jadhwani, Akash Singh
 */
public class MapperTask {
	private static final Logger LOG = LogManager.getLogger(MapperTask.class);
	private static final String CRLF = "\r\n";
	private static final String EOF = "EOF";

	@SuppressWarnings (value="rawtypes")
	private final Mapper mapper;
	private final String inputDirName;
	private final String partitionIP;
	private final int partitionPort;
	private final int id;
	private final int total;

	/**
	 * Constructor for MapperTask
	 * @param mapperClassName: User defined Mapper class name
	 * @param inputDirName: Input Directory
	 * @param id: Mapper ID
	 * @param total: Number of mappers
	 * @param partitionIP: Partitioner IP Address
	 * @param partitionPort: Partitioner port number
	 * @throws Exception
	 */
	@SuppressWarnings (value="rawtypes")
	public MapperTask(String mapperClassName, String inputDirName, int id, int total, String partitionIP, int partitionPort) throws Exception {
		this.mapper = (Mapper) Class.forName(mapperClassName).newInstance();
		this.inputDirName = inputDirName;
		this.id = id;
		this.total = total;
		this.partitionIP = partitionIP;
		this.partitionPort = partitionPort;
	}

	/**
	 * Gets the files to process from S3 or local input directory and sends for processing.
	 */
	@SuppressWarnings (value="rawtypes")
	private void mapInputFiles() {
		Socket socket = null;
		try {
			// Listener may take a few seconds to come up.
			int maxWaitSecs = 600;
			while (maxWaitSecs-- > 0) {
				try {
					socket = new Socket(partitionIP, partitionPort);
					break;
				} catch (Exception e) {
					Thread.sleep(1000);
				}
			}
			if (socket == null) {
				throw new ConnectException("Connection refused");
			}
			// Create stream to write to Partitioner's socket
			DataOutputStream outputStream = new DataOutputStream(socket.getOutputStream());

			// Implemented MapperContext to write output to Partitioner's socket directly
			Context context = new Context() {
				@Override
				public synchronized void write(Object key, Object value) throws IOException {
					outputStream.writeBytes("" + key + "," + value + CRLF);
				}
			};

			//Process input files from AWS S3 bucket
			if(inputDirName.startsWith("s3://"))
			{
				//Sample Input: s3://cs6240sp16/climate/
				String[] r = inputDirName.split("/");
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
				// Get list of all files from the input bucket and sub-directory
				ObjectListing objectListing = s3client.listObjects(new ListObjectsRequest()
						.withBucketName(bucketName)
						.withPrefix(key));
				List<S3ObjectSummary> summaries = objectListing.getObjectSummaries();

				// Process only selected files as per the given start and end range; index starts from 1
				for (int i = 0; i < summaries.size(); i++) {
					// Ignore empty files
					if(summaries.get(i).getSize() == 0)
						continue;
					// Get file data stream from S3
					if(i % total == id - 1)
					{
						LOG.info("Processing: " + "s3://" + bucketName + "/" + summaries.get(i).getKey());
						S3Object object = s3client.getObject(new GetObjectRequest(bucketName, summaries.get(i).getKey()));
						GZIPInputStream objectData = new GZIPInputStream(object.getObjectContent());
						processFileData(objectData, mapper, context);
						objectData.close();
					}
				}
			}
			else
			{
				File inputDir = new File(inputDirName);
				if (! (inputDir.exists() && inputDir.isDirectory())) {
					// must be directory
					return;
				}

				File[] files = inputDir.listFiles();
				Arrays.sort(files);
				// Process only selected files as per the given start and end range; index starts from 0
				for (int i = 0; i < files.length; i++) {

					if (files[i].isDirectory()) {
						// not recursive
						continue;
					} 
					if(i % total == id - 1)
					{
						GZIPInputStream objectData = new GZIPInputStream(new FileInputStream(files[i]));
						processFileData(objectData, mapper, context);
						objectData.close();
					}
				}					
			}
			// Send EOF signal to partitioner
			outputStream.writeBytes(EOF + CRLF);
			outputStream.close();
			socket.close();
			LOG.info("Mapper socket closed.");

		} catch (Exception e) {
			LOG.error("Inside mapInputFiles", e);
			return;
		}
		finally {
			if (socket != null) {
				try {socket.close();} catch (IOException e) {}
			}
		}
	}

	/**
	 * Reads input file and call user defined Map function for each line
	 * @param objectData: Input file stream
	 * @param mapper: User defined mapper object
	 * @param context: Mapper Context
	 * @throws IOException
	 */
	@SuppressWarnings (value={"unchecked","rawtypes"})
	private void processFileData(GZIPInputStream objectData, Mapper mapper, Context context) throws IOException {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader(objectData));
			long ix = 0;
			String line;
			while ((line = br.readLine()) != null) 
			{
				// Call user defined map function  
				mapper.map(new Text(ix), new Text(line), context);
				ix++;
			}
			br.close();
		} catch (Exception ex) {
			LOG.error("Inside processFileData", ex);
		}
	}

	public static void main(String[] args) {
		if (args.length < 5) {
			System.out.println("Wrong # of command line parameters.");
			LOG.error("Incorrect # of command line parameters.");
			return;
		}
		try {
			String[] ipAndPort = args[4].split(":");
			MapperTask mapperTask = new MapperTask(args[0], args[1], Integer.parseInt(args[2]), Integer.parseInt(args[3]), ipAndPort[0], Integer.parseInt(ipAndPort[1]));		
			mapperTask.mapInputFiles();
		} catch (Exception e) {
			LOG.error("", e);
			System.exit(1);
		}

		LOG.info("Complete.");
		System.exit(0);
	}
}
