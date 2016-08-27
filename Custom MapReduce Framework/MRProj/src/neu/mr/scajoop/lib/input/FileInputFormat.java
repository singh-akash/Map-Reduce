package neu.mr.scajoop.lib.input;

import neu.mr.scajoop.fs.Path;
import neu.mr.scajoop.mapreduce.Job;

/**
 * SCAJoop equivalent of Hadoop FileInputFormat class.
 * @author Christopher Willig
 */
public class FileInputFormat {
	public static void addInputPath(Job job, Path path) {
		job.setInputPath(path.getPath());
	}
}
