package neu.mr.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Abstract superclass for servers with transient connections.
 * @author Joe Sackett
 */
public abstract class AbstractServerTransient implements ServerStrategy {
	private static final Logger LOG = LogManager.getLogger(AbstractServerTransient.class);
	
	/** Required subclass method to handle requests. */
	protected abstract void handleRequest(List<String> fullRequest, PrintStream writer, Server server) throws IOException;
	
	/** Processes a input request . */
	@Override
	public void processInput(Socket socket, Server server) {
		BufferedReader reader =  null;
		PrintStream writer = null;
		try {
			// Get I/O streams from the socket.
			reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			writer = new PrintStream(socket.getOutputStream());

			// Read all input from socket.
			List<String> input = new ArrayList<String>();
			do {
				// Read line by line & save in list.
				String line = reader.readLine();
				if (line != null) {
					input.add(line);
				}
			} while (reader.ready()) ;
			
			// Process request.
			if (input.size() > 0) {
				System.out.println(input.get(0));
				handleRequest(parseDelimited(input.get(0), "~"), writer, server);
			}
		} catch (Exception ex) {
			LOG.error("", ex);
		}
		finally {
			if (reader != null) {
				try {reader.close();} catch (IOException ex) {}
			}
			if (socket != null) {
				try {socket.close();} catch (IOException ex) {}
			}
		}
	}
	
	/** Parses arbitrarily delimited string. */
	private static List<String> parseDelimited(String input, String delimiters) {
		List<String> tokens = new ArrayList<String>(); 
		// Tokenize the parameters out.
    	StringTokenizer toker = new StringTokenizer(input, delimiters);
    	while (toker.hasMoreTokens()) {
    		tokens.add(toker.nextToken());
    	}
    	return tokens;
	}

}