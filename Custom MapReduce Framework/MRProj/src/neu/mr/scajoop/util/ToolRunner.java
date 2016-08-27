package neu.mr.scajoop.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import neu.mr.scajoop.conf.Configuration;

/**
 * SCAJoop equivalent of Hadoop ToolRunner class.
 * @author Christopher Willig, Akash Singh
 */
public class ToolRunner {	
	/**
	 * Reads in the cluster_conf file (location stored in classpath) and sets new configuration object's fields appropriately. 
	 * @param tool
	 * @param args
	 */
	public static void run(Tool tool, String[] args) {
		Configuration conf = new Configuration();
		try {
			InputStream confFileIn = ToolRunner.class.getClassLoader().getResourceAsStream("cluster_conf"); //Requires that the DIRECTORY of the cluster configuration file is in the classpath.
			BufferedReader br = new BufferedReader(new InputStreamReader(confFileIn));
			String line;
			while((line = br.readLine()) != null) {
				if (line.length() == 0 || line.startsWith("#")) {
					continue;
				}
				String[] confLineSplit = line.split("=");
				String name = confLineSplit[0];
				String value = confLineSplit[1];
				if(name.equals("aws_keyname")) {
					conf.setAwsKey(value);
				} else if(name.equals("aws_securitygroup")) {
					conf.setSecurityGroup(value);
				} else if(name.equals("aws_instancetype")) {
					conf.setInstanceType(value);
				} else if(name.equals("aws_instancenum")) {
					conf.setNumMachines(Integer.parseInt(value));
				} else if(name.equals("mode")) {
					conf.setAwsMode(value.equals("aws"));
				}
			}
			br.close();
		} catch (IOException e) {
			throw new Error(e);
		}
		tool.setConf(conf);
		tool.run(args);
	}
}
