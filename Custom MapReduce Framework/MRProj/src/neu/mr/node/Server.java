package neu.mr.node;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Generic server used for spawning a multi-threaded server.
 * Behavior parameterized with different strategies.
 * @author Joe Sackett
 */
public class Server implements Runnable {
	private static final Logger LOG = LogManager.getLogger(Server.class);
	
	/** Port bound to by this server. */
	protected int serverPortNum;
	
	/** Strategy executed by workers processing connections to this server (web server or back channel). */
	protected ServerStrategy serverStrategy;
	
	/** Server listener socket. */
	protected ServerSocket serverSocket = null;
	
	/** Control switch to shutdown this server instance. */
	protected boolean controlSwitch = true;
	
	public Server(int serverPortNum, ServerStrategy serverStrategy) {
		this.serverPortNum = serverPortNum;
		this.serverStrategy = serverStrategy;
	}
	
	public int getServerPortNum() {
		return serverPortNum;
	}

	public boolean isControlSwitch() {
		return controlSwitch;
	}

	public void setControlSwitch(boolean controlSwitch) {
		this.controlSwitch = controlSwitch;
	}

	public void shutdown() {
		setControlSwitch(false);
	}

	public void setServerStrategy(ServerStrategy serverStrategy) {
		this.serverStrategy = serverStrategy;
	}
	
	/** Thread run method. Terminates upon completion. */
	@Override
	public void run() {
		LOG.info("Starting " + serverStrategy.getTypeName() + " listener on port: " + getServerPortNum());
		try {
			serverSocket = new ServerSocket(serverPortNum);
			while (isControlSwitch()) {
				// Wait for the next client connection.
				Socket socket = serverSocket.accept();
				// Spawn thread, along with strategy defining logic.
				new Thread(new Worker(socket, serverStrategy, this)).start();
			}
		} catch (Exception ex) {
			LOG.error("", ex);
		}
		finally {
			if (serverSocket != null) {
				LOG.info("Closing socket.");
				try { serverSocket.close(); } catch (IOException ex) {}
			}
		}
		LOG.info(serverStrategy.getTypeName() + " at: " + getServerPortNum() + " listener exiting.");
	}
	
}
