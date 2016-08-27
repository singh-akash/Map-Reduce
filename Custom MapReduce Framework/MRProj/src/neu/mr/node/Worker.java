package neu.mr.node;

import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Generic worker spawned as thread to handle inbound request.
 * Behavior parameterized with different strategies.
 * @author Joe Sackett
 */
public class Worker implements Runnable {
	private static final Logger LOG = LogManager.getLogger(AbstractServerPersistent.class);
	
	/** Socket connected to the client whom this worker will process. */
	private Socket socket;
	
	/** Strategy executed by workers processing connections to this server. */
	private ServerStrategy serverStrategy;
	
	/** Spawning server. */
	private Server server;
	
	public Worker(Socket socket, ServerStrategy serverStrategy, Server server) {
		this.socket = socket;
		this.serverStrategy = serverStrategy;
		this.server = server;
	}

	/** Method to execute when thread is spawned. */
	@Override
	public void run() {
		LOG.info("Spawning " + serverStrategy.getTypeName() + " worker at: " + server.getServerPortNum() + " to process request.");
		serverStrategy.processInput(socket, server);
	}
}