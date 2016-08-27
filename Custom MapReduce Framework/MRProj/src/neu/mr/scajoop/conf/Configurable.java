package neu.mr.scajoop.conf;

/**
 * SCAJoop equivalent of Hadoop Configurable class.
 * @author Christopher Willig
 */
public interface Configurable {
	void setConf(Configuration conf);
	Configuration getConf();
}
