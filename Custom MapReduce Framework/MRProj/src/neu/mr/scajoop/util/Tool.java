package neu.mr.scajoop.util;

import neu.mr.scajoop.conf.Configurable;

/**
 * SCAJoop equivalent of Hadoop Tool class.
 * @author Christopher Willig, Akash Singh
 */
public interface Tool extends Configurable {
	public void run(String[] args);
}
