package neu.mr.scajoop.mapreduce;

import neu.mr.scajoop.Context;

/** 
 * SCAJoop Mapper Abstract Interface.
 * @author Joe Sackett
 */
public abstract class Mapper<K1, V1, K2, V2> {

	/** Must be implemented by mappers. */
 	public abstract void map(K1 key, V1 value, Context<K2,V2> context);

}
