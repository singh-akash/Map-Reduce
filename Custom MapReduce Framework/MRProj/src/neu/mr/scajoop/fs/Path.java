package neu.mr.scajoop.fs;

/**
 * SCAJoop equivalent of Hadoop Path class.
 * @author Christopher Willig
 */
public class Path {
	private String path;
	
	public Path(String path) {
		this.path = path;
	}
	
	public String getPath() {
		return path;
	}
}