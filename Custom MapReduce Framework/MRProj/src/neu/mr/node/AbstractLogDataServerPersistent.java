package neu.mr.node;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;

import org.apache.logging.log4j.core.impl.Log4jLogEvent;

/**
 * @author Christopher Willig
 * Same as AbstractServerTransient except that it reads in an object, casts that object to a Log4jLogEvent and passes off that object instead of a list of strings.
 */
public abstract class AbstractLogDataServerPersistent implements ServerStrategy {
	protected abstract void handleInput(Log4jLogEvent msgEvent) throws IOException;

	@Override
	public void processInput(Socket socket, Server server) {
		ObjectInputStream ois = null;
		try {
			ois = new ObjectInputStream(socket.getInputStream());
			Object readObject;
			
			while((readObject = ois.readObject()) != null) {
				Log4jLogEvent msgEventObj = (Log4jLogEvent)readObject;
				handleInput(msgEventObj);
			}
		} catch (IOException | ClassNotFoundException ex) {
			ex.printStackTrace();
		} finally {
			if(ois != null) {
				try {
					ois.close();
				} catch (IOException e) {

				}
			}
			if(socket != null) {
				try {
					socket.close();
				} catch (IOException e) {
					throw new Error(e);
				}
			}
		}
	}

}
