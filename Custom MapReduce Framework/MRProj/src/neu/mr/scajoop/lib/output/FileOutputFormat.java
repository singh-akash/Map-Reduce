package neu.mr.scajoop.lib.output;

import neu.mr.scajoop.fs.Path;
import neu.mr.scajoop.mapreduce.Job;

/**
 * SCAJoop equivalent of Hadoop FileOutputFormat class.
 * @author Christopher Willig
 */
public class FileOutputFormat {
	public static void setOutputPath(Job job, Path path) {
		job.setOutputPath(path.getPath());
	}
}
