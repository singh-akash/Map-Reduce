package neu.mr.scajoop;

/**
 * @author Christopher Willig
 */
public class Connection {
	public String addr;
	public int port;
	
	public Connection(String addr, int port) {
		this.addr = addr;
		this.port = port;
	}
	
	public String toString() {
		return addr + "/" + port;
	}
}
