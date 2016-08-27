package neu.mr.scajoop.mapreduce;

import java.io.IOException;
import java.net.Socket;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.SecurityGroup;

import neu.mr.scajoop.Connection;
import neu.mr.scajoop.JobManager;
import neu.mr.scajoop.conf.Configuration;

/**
 * SCAJoop equivalent of Hadoop Job class.
 * @author Christopher Willig
 */
public class Job {
	private static final int NODE_MGR_PORT = 12345;
	private static final JobManager MANAGER = new JobManager();

	private String jobName;
	private String jarName;
	private Configuration conf;
	private Class<?> mainCls;
	private Class<?> outputKeyCls;
	private Class<?> outputValueCls;
	private Class<?> mapOutputKeyCls;
	private Class<?> mapOutputValueCls;
	private Class<? extends Mapper<?, ?, ?, ?>> mapperCls;
	private Class<? extends Reducer<?, ?, ?, ?>> reducerCls;

	private String inputPath;
	private String outputPath;

	private Job() {	}

	public static Job getInstance(Configuration conf, String name) {
		Job job = new Job();
		job.conf = conf;
		job.jobName = name;
		return job;
	}

	public Configuration getConfiguration() {
		return this.conf;
	}

	public void setJarByClass(Class<?> cls) {
		this.mainCls = cls;
	}

	public void setMapperClass(Class<? extends Mapper<?, ?, ?, ?>> cls) {
		this.mapperCls = cls;
	}

	public void setReducerClass(Class<? extends Reducer<?, ?, ?, ?>> cls) {
		this.reducerCls = cls;
	}

	public void setOutputKeyClass(Class<?> cls) {
		this.outputKeyCls = cls;
	}

	public void setOutputValueClass(Class<?> cls) {
		this.outputValueCls = cls;
	}

	public void setMapOutputKeyClass(Class<?> cls) {
		this.mapOutputKeyCls = cls;
	}

	public void setMapOutputValueClass(Class<?> cls) {
		this.mapOutputValueCls = cls;
	}

	public void setOutputPath(String path) {
		this.outputPath = path;
	}

	public void setInputPath(String path) {
		this.inputPath = path;
	}

	public void setJar(String jarName) {
		this.jarName = jarName;
	}

	/**
	 * Core function to the SCAJoop framework. This will start a cluster (local or AWS) and will launch a job on that cluster. It will also continuously read log messages until the reducers finish running. 
	 * This method blocks until the reducers finish running.
	 * @param verbose Not used in current version
	 * @return Always true.
	 */
	public boolean waitForCompletion(boolean verbose) {
		//This is actually going to launch the cluster.
		//1.) Create cluster.
		//2.) Send all commands to all NodeManagers
		//3.) Read log messages from all NodeManagers (fork #_of_nodes threads. Each thread polls one NM for logs and immediately prints them to console. Do thread.join at the end to keep synchronization).
		int numNodes = this.conf.getNumMachines();
		String keyName = this.conf.getAwsKey();
		String securityGroup = this.conf.getSecurityGroup();
		String instanceType = this.conf.getInstanceType();
		ArrayList<String> msgs = new ArrayList<String>();

		AmazonEC2Client awsClient = null;
		
		//Creates cluster and starts node managers on each node.
		if(conf.isAwsMode()) {
			//Start aws cluster
			awsClient = this.startAWSCluster(numNodes, keyName, securityGroup, instanceType, NODE_MGR_PORT);
			constructAWSNMCommands(numNodes, msgs); //Construct/Get NodeManager commands.
		} else {
			//Start local cluster
			//1 node manager = 1 node (or, computer/instance in aws). Therefore, to have a 4 node cluster, 4 NodeManagers are required to be running.
			this.startLocalCluster(numNodes, NODE_MGR_PORT);
			constructLocalNMCommands(numNodes, msgs); //Construct/Get NodeManager commands.
		}

		//Send Reducer, Parititioner, and Mapper commands.
		for(String msg : msgs) {
			try {
				MANAGER.sendNodeManagerMessage(msg);
			} catch (IOException e) {
				throw new Error(e);
			}
		}

		//Read logs from all NodeManagers.
		readStatus();	
		
		//Terminate all AWS nodes (if cluster is an AWS cluster)
		if(awsClient != null)
			MANAGER.terminateAllAWSNodes(awsClient);
		
		return true;
	}

	/**
	 * Constructs all commands to be sent to the NodeManager/s that tell it to start a Reducer, Partitioner, and Mapper (in that order). This is for a local cluster.
	 * Since a local cluster's NodeManagers run on the different ports but on the same IP addresse, this function is local cluster specific (compared to AWS which is different IPs same port numbers).
	 * @param numNodes
	 * @param msgs List containing all messages to be sent to the NodeManager/s.
	 */
	private void constructLocalNMCommands(int numNodes, ArrayList<String> msgs) {
		String classpath = getClasspath();

		//Construct reduce task messages
		for(int i = 1; i <= numNodes; i++) {
			int newPort = (8300 + i) - 1;
			String reducerMsg = "start~" + classpath + "~-Xms256m~-Xmx16g~neu.mr.scajoop.ReducerTask~" + this.reducerCls.getName() + "~" + newPort + "~" + i + "~" + this.outputPath + "~" + i;
			msgs.add(reducerMsg);
		}

		//Construct partition task message. When running locally, different ports need to be taken into account.
		String postfix = "";
		for(int i = 0; i < numNodes; i++) {
			int newPort = 8300 + i;
			postfix += "IP_ADDRESS_1:" + newPort + "~";
		}
		postfix = postfix.substring(0, postfix.length() - 1); //Remove last tilde.
		String partitionerMsg = "start~" + classpath + "~-Xms256m~-Xmx1g~neu.mr.scajoop.PartitionTask~8200~" + numNodes + "~" + postfix + "~1";
		msgs.add(partitionerMsg);

		//Construct local version of commands.
		//Mapper
		for(int i = 1; i <= numNodes; i++) {
			String mapperMsg = "start~" + classpath + "~-Xms256m~-Xmx8g~neu.mr.scajoop.MapperTask~" + this.mapperCls.getName() + "~" + this.inputPath + "~" + i + "~" + numNodes + "~IP_ADDRESS_1:8200~" + i;
			msgs.add(mapperMsg);
		}
	}

	/**
	 * Constructs all commands to be sent to the NodeManager/s that tell it to start a Reducer, Partitioner, and Mapper (in that order). This is for an AWS cluster.
	 * Since an AWS cluster's NodeManagers run on the same port but on different IP addresses, this function is AWS cluster specific (compared to local which is same IP different port numbers).
	 * @param numNodes
	 * @param msgs List containing all messages to be sent to the NodeManager/s.
	 */
	private void constructAWSNMCommands(int numNodes, ArrayList<String> msgs) {
		//Constructs the command messages and sends them to the node manager.
		//Construct reduce task messages
		for(int i = 1; i <= numNodes; i++) {
			String reducerMsg = "start~/home/ec2-user/SCAJoop.jar:/home/ec2-user/" + this.jarName + "~-Xms512m~-Xmx16g~neu.mr.scajoop.ReducerTask~" + this.reducerCls.getName() + "~8300~" + i + "~" + this.outputPath + "~" + i;
			msgs.add(reducerMsg);
		}

		//Construct partition task message
		String partitionerMsg = "start~/home/ec2-user/SCAJoop.jar~-Xms512m~-Xmx2g~neu.mr.scajoop.PartitionTask~8200~" + numNodes + "~APPENDALLIP:8300~1";
		msgs.add(partitionerMsg);

		//Construct mapper task message
		for(int i = 1; i <= numNodes; i++) {
			//start~FRAMEWORK.jar~USER_PROGRAM.jar~neu.mr.scajoop.MapperTask~USER_MAPPER_PATH~input~...
			//start~SCAJoop.jar~A2.jar~neu.mr.scajoop.MapperTask~neu.mr.UserMapper~input~.....
			String mapperMsg = "start~/home/ec2-user/SCAJoop.jar:/home/ec2-user/" + this.jarName + "~-Xms512m~-Xmx16g~neu.mr.scajoop.MapperTask~" + this.mapperCls.getName() + "~" + this.inputPath + "~" + i + "~" + numNodes + "~IP_ADDRESS_1:8200~" + i;
			msgs.add(mapperMsg);
		}
	}

	/**
	 * Gets the status and new log messages of the job.
	 * @throws Error
	 */
	private void readStatus() throws Error {
		//Reads log messages from all node managers.
		ArrayList<Thread> logThreads = new ArrayList<Thread>();
		for(Connection conn : MANAGER.getConnections()) {
			Thread logThread = new Thread() {
				//Connection with NodeManager is always transient. So each thread that polls a NodeManager needs to continuously do so (at 1 second intervals).
				public void run() {
					while(true) {
						try {
							MANAGER.getLog(conn);
							if(MANAGER.jobIsComplete(conn))
								return;
							Thread.sleep(1000);
						} catch (InterruptedException | IOException e) {
							throw new Error(e);
						}
					}
				}
			};
			logThreads.add(logThread);
			logThread.start();
		}

		//This should not terminate (currently) since each thread has an open while loop.
		for(int i = 0; i < logThreads.size(); i++) {
			try {
				logThreads.get(i).join();
			} catch (InterruptedException e) {
				throw new Error(e);
			}
		}
	}

	/**
	 * Starts a local cluster. Each "node" is a node manager running on a different port.
	 * @param numNodes
	 * @param startPortNum Beginning port number. Each node manager that starts will run on port startPortNum+i where i = 0 to numNodes.
	 */
	public void startLocalCluster(int numNodes, int startPortNum) {
		System.out.println("Starting Cluster...");
		for(int i = 0; i < numNodes; i++) {
			System.out.print(". ");
			String classPathStr = getClasspath();
			ProcessBuilder pb = new ProcessBuilder("java", "-cp", classPathStr, "neu.mr.node.NodeManager", Integer.toString(startPortNum + i));
			try {
				pb.start();
				Thread.sleep(4000);
			} catch (IOException | InterruptedException e) {
				throw new Error(e);
			}
			MANAGER.addConnection(new Connection("127.0.0.1", startPortNum + i));
		}
		System.out.println("\nCluster started!");
	}

	private String getClasspath() {
		URL[] classPaths = ((URLClassLoader) Thread.currentThread().getContextClassLoader()).getURLs();
		String classPathStr = "";
		for(URL cp : classPaths) 
			classPathStr += cp.toString() + ":";
		classPathStr = classPathStr.substring(0, classPathStr.length() - 1); //Remove last colon
		return classPathStr;
	}

	/**
	 * Creates/Starts an AWS EC2 cluster. Also starts node managers on each instance.
	 * @param numNodes Number of nodes in cluster.
	 * @param keyName AWS key name (found here: https://console.aws.amazon.com/ec2/v2/home?region=us-east-1#KeyPairs:sort=keyName)
	 * @param securityGroup Name of security group. Must be world accessible on all ports and by any IP address (CIDR notation: 0.0.0.0/0)!
	 * @param instanceType Name of instance type. In console, type `help` to get list of types.
	 * @param nodeMgrPort Port for node manager to run on (when ran on AWS instance).
	 * @return An EC2Client object. Used for cluster/node termination.
	 * @throws InterruptedException
	 */
	public AmazonEC2Client startAWSCluster(int numNodes, String keyName, String securityGroup, String instanceType, int nodeMgrPort) {
		System.out.println("Starting Cluster...");
		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		AmazonEC2Client client = new AmazonEC2Client(credentials);
		client.setEndpoint("ec2.us-east-1.amazonaws.com");

		DescribeSecurityGroupsRequest dsgreq = new DescribeSecurityGroupsRequest().withGroupNames(securityGroup); 
		DescribeSecurityGroupsResult dsgres = client.describeSecurityGroups(dsgreq);
		SecurityGroup group = dsgres.getSecurityGroups().get(0);

		RunInstancesRequest request = new RunInstancesRequest().withImageId("ami-bd3a31d7").withInstanceType(instanceType).withMinCount(1).withMaxCount(numNodes).withKeyName(keyName).withUserData(getUserDataScript(this.jarName)).withSecurityGroupIds(group.getGroupId());
		RunInstancesResult result = client.runInstances(request); //Note that sometimes, a 503 (Service is Unavailable. Please try again shortly.) error will occur when trying to launch an instance.

		//Wait until all nodes have started and all node managers are available on port 12345
		List<String> addrs;
		while((addrs = allStarted(client, result.getReservation().getInstances(), nodeMgrPort)) == null) {
			System.out.print(". ");
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e) {
				throw new Error(e);
			}
		}
		System.out.println("\nCluster started!");

		//Add the connections to the admin connections
		for(String addr : addrs) 
			MANAGER.addConnection(new Connection(addr, nodeMgrPort));

		return client;
	}

	//Codes (from AWS SDK doc):
	//0 : pending
	//16 : running
	//32 : shutting-down
	//48 : terminated
	//64 : stopping
	//80 : stopped
	private static List<String> allStarted(AmazonEC2Client client, List<Instance> instances, int nodeMgrPort) {
		List<String> addrs = new ArrayList<String>(); //Contains a list of the IP addresses of each node in the cluster.
		DescribeInstancesResult dir = client.describeInstances();
		for(Reservation r : dir.getReservations()) {
			//Instance i = instances gotten from the client (which are fetched from AWS).
			//Instance j = all instances started from the client.runInstances() request above.
			for(Instance i : r.getInstances()) {
				for(Instance j : instances) {
					if(i.getInstanceId().equals(j.getInstanceId())) {
						//16 = running (see above table).
						if(i.getState().getCode() != 16) {
							return null;
						} else {
							//Make sure that the instance's port 12345 is open.
							String addr = i.getPublicIpAddress();
							try{
								Socket sock = new Socket(addr, nodeMgrPort);
								sock.close();
								//if check is required here because we're going over instances multiple times (nested for loops).
								if(!addrs.contains(i.getPublicIpAddress()))
									addrs.add(i.getPublicIpAddress());
							} catch(Exception ex) {
								return null;
							}
						}
					}
				}
			}
		}

		return addrs;
	}

	/**
	 * Converts EC2 instance boot script to a base 64 encoded string. Script will pull latest SCAJoop.jar file from S3 and starts the node manager.
	 * @return Base 64 encoded string version of EC2 instance boot script.
	 */
	private static String getUserDataScript(String userJarName){
		ArrayList<String> lines = new ArrayList<String>();
		lines.add("#!/bin/bash");
		lines.add("aws s3 cp s3://ascj-bucket/SCAJoop.jar /home/ec2-user");
		lines.add("aws s3 cp s3://ascj-bucket/" + userJarName + " /home/ec2-user");
		lines.add("chmod 777 /home/ec2-user/SCAJoop.jar");
		lines.add("export JAVA_OPTS=\"-Xms1g -Xmx5g\"");
		lines.add("java -cp /home/ec2-user/SCAJoop.jar neu.mr.node.NodeManager " + NODE_MGR_PORT + "");
		String str = new String(Base64.encodeBase64(join(lines, "\n").getBytes()));
		return str;
	}

	private static String join(Collection<String> s, String delimiter) {
		StringBuilder builder = new StringBuilder();
		Iterator<String> iter = s.iterator();
		while (iter.hasNext()) {
			builder.append(iter.next());
			if (!iter.hasNext()) {
				break;
			}
			builder.append(delimiter);
		}
		return builder.toString();
	}
}
