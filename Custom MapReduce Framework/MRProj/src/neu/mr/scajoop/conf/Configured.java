package neu.mr.scajoop.conf;

/**
 * SCAJoop equivalent of Hadoop Configured class.
 * @author Christopher Willig
 */
public abstract class Configured implements Configurable {
	private Configuration conf;
	
	public Configured() { }
	
	public Configuration getConf() {
		return this.conf;
	}
	
	public void setConf(Configuration conf) {
		this.conf = conf;
	}
}
