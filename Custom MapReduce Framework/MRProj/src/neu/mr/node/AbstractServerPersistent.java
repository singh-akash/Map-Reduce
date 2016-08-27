package neu.mr.node;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Abstract superclass for servers with persistent connections.
 * @author Joe Sackett
 */
public abstract class AbstractServerPersistent implements ServerStrategy {
	private static final Logger LOG = LogManager.getLogger(AbstractServerPersistent.class);
	
	/** Required subclass method to handle single inputs. */
	protected abstract void handleInput(String input, Reader reader) throws IOException;
	
	/** Processes a input request . */
	@Override
	public void processInput(Socket socket, Server server) {
		BufferedReader reader =  null;
		PrintStream writer = null;
		try {
			// Get I/O streams from the socket.
			reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
			writer = new PrintStream(socket.getOutputStream());

			// Read input from socket and process incrementally.
			String line;
			while ((line = reader.readLine()) != null) {
				handleInput(line, reader);
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
}