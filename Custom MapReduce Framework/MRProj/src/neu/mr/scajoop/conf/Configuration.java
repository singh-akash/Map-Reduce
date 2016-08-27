package neu.mr.scajoop.conf;

/**
 * SCAJoop equivalent of Hadoop Configuration class.
 * @author Christopher Willig
 */
public class Configuration {
	//POJO for storing configuration values.
	private String awsKey;
	private String securityGroup;
	private String instanceType;
	private int	numMachines;
	private boolean awsMode; //aws or local
	
	public Configuration() { }

	public String getAwsKey() {
		return awsKey;
	}

	public void setAwsKey(String awsKey) {
		this.awsKey = awsKey;
	}

	public String getSecurityGroup() {
		return securityGroup;
	}

	public void setSecurityGroup(String securityGroup) {
		this.securityGroup = securityGroup;
	}

	public String getInstanceType() {
		return instanceType;
	}

	public void setInstanceType(String instanceType) {
		this.instanceType = instanceType;
	}

	public int getNumMachines() {
		return numMachines;
	}

	public void setNumMachines(int numMachines) {
		this.numMachines = numMachines;
	}

	public boolean isAwsMode() {
		return awsMode;
	}

	public void setAwsMode(boolean awsMode) {
		this.awsMode = awsMode;
	}
}
