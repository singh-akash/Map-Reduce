package neu.mr.scajoop.mapreduce;

import neu.mr.scajoop.Context;

/** 
 * SCAJoop Reducer Abstract Interface.
 * @author Joe Sackett
 */
public abstract class Reducer<K1,V1,K2,V2> {

	/** Must be implemented by reducers. */
 	public abstract void reduce(K1 key, Iterable<V1> values, Context context);
 	
}
