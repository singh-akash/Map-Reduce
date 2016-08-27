package neu.mr.node;

import java.io.IOException;
import java.io.PrintStream;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;

/**
 * Responsible for starting tasks and managing them.
 * @author Joe Sackett
 */
public class NodeManager extends Server {
	private static final int LOGGER_DATA_PORT = 6666;
	private static final Logger LOG_HANDLE = LogManager.getLogger(NodeManager.class);
	public static final List<String> LOG = Collections.synchronizedList(new ArrayList<String>()); //Log/Buffer that stores log messages.
	public static Thread reducerWatcher = null;

	public NodeManager(int adminPortNum, ServerStrategy serverStrategy) {	
		super(adminPortNum, serverStrategy);
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 1) {
			System.out.println("Wrong # of command line parameters.");
			return;
		}

		int adminPortNum = Integer.parseInt(args[0]);
		NodeManager nodeManager = new NodeManager(adminPortNum, new NodeManagerStrategy());
		Server loggerDataServer = new Server(LOGGER_DATA_PORT, new LoggerDataStrategy()); //NOTE: When running in local mode, only 1 NM will have a logger data port open. This is because if 5 NMs open a port on 6666, only one can be open on it. All logs from all processes will log to this NM.
		Thread nmThread = new Thread(nodeManager);
		Thread logThread = new Thread(loggerDataServer);
		logThread.start();
		nmThread.start();
	}

	/** Implementation for Node Manager. */
	private static class NodeManagerStrategy extends AbstractServerTransient {

		public NodeManagerStrategy() {
		}

		/** Echo name specific for this app. */
		@Override
		public String getTypeName() {
			return "Node Manager";
		}

		/** Process request string & delegate to handler functions. */
		@Override
		protected void handleRequest(List<String> fullRequest, PrintStream writer, Server server) throws IOException {
			// Parse & validate request.
			String request = fullRequest.get(0);
			
			if ("shutdown".equalsIgnoreCase(request) || "kill".equalsIgnoreCase(request)) {
				LOG_HANDLE.info("Shutting down Node Manager.");
				server.shutdown();
				System.exit(0);
			}
			else if ("start".equalsIgnoreCase(request)) {
				startTask(fullRequest);
			}
			else if ("get".equalsIgnoreCase(request) && "logs".equalsIgnoreCase(fullRequest.get(1))) {
				//Print logs back to admin.
				synchronized (NodeManager.LOG) {
					Iterator<String> i = NodeManager.LOG.iterator();
					while(i.hasNext()) 
						writer.println(i.next());
					writer.flush();
					NodeManager.LOG.clear(); //Required since if the buffer isn't cleared each time logs are fetched, duplicates will be shown.
				}
			}
			else if("get".equalsIgnoreCase(request) && "status".equalsIgnoreCase(fullRequest.get(1))) {
				if(NodeManager.reducerWatcher == null || NodeManager.reducerWatcher.isAlive()) {
					writer.println("INCOMPLETE");
					writer.flush();
				} else {
					writer.println("COMPLETE");
					writer.flush();
				}
			}
			else {
				LOG_HANDLE.warn("Command not recognized.");
			}
		}

		/** Starts a task as child process. */
		private static void startTask(List<String> taskRequest) {
			if (taskRequest.size() < 3) {
				return;
			}

			ProcessBuilder processBuilder = new ProcessBuilder(packVarArgs("java", "-cp", taskRequest, 1));
			LOG_HANDLE.info(Arrays.toString(packVarArgs("java", "-cp", taskRequest, 1)));
			try {
				Process p = processBuilder.start();
				//java -cp blah.jar:blah2.jar -Xms1g -Xmx10g neu.mr.scajoop.ReducerTask ...
				//Construct a thread for each reducer. The thread will simply wait for the processes to die. Once they all die, the job is complete.
				if(processBuilder.command().get(5).equals("neu.mr.scajoop.ReducerTask")) {
					NodeManager.reducerWatcher = new Thread(new Runnable() {						
						@Override
						public void run() {
							try {
								p.waitFor();
							} catch (InterruptedException e) {
								throw new Error(e);
							}
						}
					});
					NodeManager.reducerWatcher.start();
				}
			} catch (Exception e) {
				LOG_HANDLE.error("", e);
			}
		}

		private static final int numStaticPack = 2;
		/** Helper method to organize application's command line arguments for process execution. */
		private static String[] packVarArgs(String arg0, String arg1, List<String> command, int offset) {
			String[] argsOut = new String[command.size() + numStaticPack - offset];
			argsOut[0] = arg0;
			argsOut[1] = arg1;
			for (int ix = numStaticPack ; ix < command.size() + numStaticPack - offset ; ix++) {
				argsOut[ix] = command.get(ix - numStaticPack + offset);
			}
			return argsOut;
		}
	}

	private static class LoggerDataStrategy extends AbstractLogDataServerPersistent {

		public LoggerDataStrategy() { }

		@Override
		public String getTypeName() {
			return "Logger Data";
		}

		@Override
		protected void handleInput(Log4jLogEvent msgEvent) throws IOException {
			Format format = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			//Take object and put data into LOG collection
			//For each new line in the msgEvent message, I need a new entry inside of the LOG collection.
			long time = msgEvent.getTimeMillis();
			Date date = new Date(time);
			String originClass = msgEvent.getLoggerName();
			String level = msgEvent.getLevel().name();
			String message = msgEvent.getMessage().getFormattedMessage();
			if(msgEvent.getThrownProxy() != null) {
				message += msgEvent.getThrownProxy().getExtendedStackTraceAsString();
				//message += msgEvent.getThrown().toString();
			}

			String[] splitMsg = message.split("\n");
			//Normally, a logger will log a line for each line in a message (such as an exception).
			for(String subMsg : splitMsg)
				NodeManager.LOG.add("[" + format.format(date) + "] [" + originClass + "] [" + level + "] " + subMsg);
			//System.out.println(NodeManager.LOG.toString());
		}
	}
}
