package neu.mr.scajoop;

import java.io.IOException;

/**
 * SCAJOOP Context Interface
 * @author Joe Sackett
 *
 */
public interface Context<K,V> {

	public void write(K key, V value) throws IOException;
}
