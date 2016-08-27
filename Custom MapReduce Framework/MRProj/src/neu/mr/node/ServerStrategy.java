package neu.mr.node;

import java.net.Socket;

/**
 * Interface defining the method for processing client requests.
 * Implemented by subclasses. Part of Strategy pattern.
 */
public interface ServerStrategy {
	/** Echo type name. */
	public String getTypeName();
	
	/** Processes a request. */
	public void processInput(Socket socket, Server server);
}
