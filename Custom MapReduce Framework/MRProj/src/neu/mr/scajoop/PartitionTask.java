package neu.mr.scajoop;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.net.ConnectException;
import java.net.Socket;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import neu.mr.node.AbstractServerPersistent;
import neu.mr.node.Server;

/**
 * Task to partitions data between reducers by hash code.
 * @author Akash Singh
 */
public class PartitionTask {
	private static final Logger LOG = LogManager.getLogger(PartitionTask.class);

	/** Port on which to listen for mapper connections. */
	private final int inputDataPortNum;
	/** Number of mappers from which to expect data. */
	private final int numMappers;
	/** List of IP address and ports for reducers. */
	private final List<String> reducerIPPs = new LinkedList<String>();

	public static void main(String[] args) {
		try
		{
			if (args.length < 3) {
				System.out.println("Wrong # of command line parameters.");
				LOG.error("Wrong # of command line parameters.");
				return;
			}

			PartitionTask partitionTask = new PartitionTask(args);
			
			// Start daemon thread to partition data.
			partitionTask.startDataPartitionerThread();		
		}
		catch(Exception e)
		{
			LOG.error("", e);
		}
		
	}

	public PartitionTask(String[] args) {	
		int ix = 0; //Changed from 1 to 0 (since an example argument list is: [8200, 2, 127.0.0.1:8300, 127.0.0.1:8301] and the port number is at the 0th index).
		inputDataPortNum = Integer.parseInt(args[ix++]);
		numMappers = Integer.parseInt(args[ix++]);
		for (; ix < args.length ; ix++) {
			reducerIPPs.add(args[ix]);
		}
	}

	/** Starts daemon thread to partition data. 
	 * @throws InterruptedException */
	private void startDataPartitionerThread() throws InterruptedException {
		Server partitionTask = new Server(inputDataPortNum, new PartitionInputDataStrategy(numMappers, reducerIPPs));
		Thread thread = new Thread(partitionTask);
		thread.setDaemon(true);
		thread.start();
		thread.join();
	}
	
	/** Strategy to processes input data to Partitioner. */
	private static class PartitionInputDataStrategy extends AbstractServerPersistent {
		/** Number of mappers still alive. */
		private int aliveMappers;
		/** Number of reducers. */
		private final int numReducers;
		/** Sockets connected to reducers. */
		private Socket[] reducerSockets;
		/** Streams around sockets connected to reducers. */
		private DataOutputStream[] reducerStreams;

		public PartitionInputDataStrategy(int numMappers, List<String> reducerIPPs) {
			this.aliveMappers = numMappers;
			this.numReducers = reducerIPPs.size();
			try {
				// Connect to reducers.
				int ix = 0;
				reducerSockets = new Socket[numReducers];
				reducerStreams = new DataOutputStream[numReducers];
				for (String reducerIPP : reducerIPPs) {
					String[] ipAndPort = reducerIPP.split(":");
					reducerSockets[ix] = openWithTimeout(ipAndPort[0], Integer.parseInt(ipAndPort[1]));
					reducerStreams[ix] = new DataOutputStream(reducerSockets[ix].getOutputStream());
					ix++;
				}
			} catch (Exception e) {
				LOG.error("", e);
				return;
			}
		}
		
		/** Connects to a socket with 10 retries. */
		private static Socket openWithTimeout(String partitionIP, int partitionPort) throws Exception {
			// Listener may take a few seconds to come up.
			Socket socket = null;
			int maxWaitSecs = 10;
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
			return socket;
		}
		
		/** Closes all streams and sockets. */
		private void cleanup() throws Exception {
			for (int ix=0 ; ix < reducerSockets.length ; ix++) {
				// write pill
				reducerStreams[ix].writeBytes(EOF + CRLF);
				// close stream and socket
				reducerStreams[ix].close();
				reducerSockets[ix].close();
			}
			LOG.info("Complete.");
			System.exit(0);
		}

		/** Echo name specific for this server. */
		@Override
		public String getTypeName() {
			return "Partitioner Data";
		}
		
		private static final String CRLF = "\r\n";
		private static final String EOF = "EOF";
		
		/** Process input data. This is the main partitioner workhorse. */
		@Override
		protected void handleInput(String input, Reader reader) throws IOException {
			try {
				if (EOF.equalsIgnoreCase(input)) {
					LOG.info("EOF received.");
					// Have all of the mappers sent EOF ?
					if (--aliveMappers == 0) {
						cleanup();
						return;
					}
				}
				String[] inputFields = input.split(",");
				if (inputFields.length < 2) {
					return;
				}
				// Get value field to partition on.
				String key = inputFields[0];
				// Find hash modulo number of reducers for index to reducer connection.
				int ix = key.hashCode() % numReducers;
				synchronized (reducerStreams[ix]) {
					reducerStreams[ix].writeBytes(input + CRLF);
				}
			} catch (Exception e) {
				LOG.error("", e);
				System.exit(0);
				return;
			}
	    }
	}
}
