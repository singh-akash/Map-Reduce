package neu.mr.scajoop;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.TerminateInstancesResult;

/**
 * Provides job function definitions. All connections are transient in here.
 * @author Christopher Willig
 */
public class JobManager {
	private ArrayList<Connection> connections = new ArrayList<Connection>(); //Connections from this program to remote server.

	public void connectToNodeManager(String ipAddress, int port) throws UnknownHostException, IOException {
		connections.add(new Connection(ipAddress, port));
	}

	/**
	 * Sends a node manager a command. Which manager is encoded in the command string.
	 * @param command
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void sendNodeManagerMessage(String command) throws UnknownHostException, IOException {
		String[] c = command.split("~");
		String newComm = "";
		if(command.contains("APPENDALLIP") || command.contains("IP_ADDRESS"))
		{
			for(int i = 0; i < c.length - 1; i++)
			{
				if(c[i].contains("APPENDALLIP"))
				{
					String port = c[i].split(":")[1];
					c[i] = "";
					for(Connection conn : connections)
					{
						c[i] += conn.addr+":"+port+"~";
					}
					c[i] = c[i].substring(0, c[i].length() - 1);
				}
				else if(c[i].contains("IP_ADDRESS"))
				{
					String port = c[i].split(":")[1];
					String nodeNum = c[i].split(":")[0].substring(c[i].lastIndexOf("_") + 1);
					c[i] = connections.get(Integer.parseInt(nodeNum) - 1).addr + ":" + port;
				}
				newComm += c[i] + "~";
			}
			newComm = newComm.substring(0, newComm.length() - 1);
		}
		else
		{
			newComm = command.substring(0, command.lastIndexOf("~"));
		}

		this.sendMessage(connections.get(Integer.parseInt(c[c.length - 1]) - 1), newComm);
	}

	/**
	 * Gets logs from logger/log-server and prints them to console.
	 * @param portNum
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	public void getLogs() throws UnknownHostException, IOException {
		for(Connection conn : connections) {
			Socket sock = new Socket(conn.addr, conn.port);
			DataOutputStream out = new DataOutputStream(sock.getOutputStream());
			Scanner in = new Scanner(sock.getInputStream());
			out.writeBytes("GET~LOGS\r\n");
			while(in.hasNextLine()) 
				System.out.println(in.nextLine());
			in.close();
			out.close();
			sock.close();
		}
	}

	public void getLog(Connection conn) {
		try{
			Socket sock = new Socket(conn.addr, conn.port);
			DataOutputStream out = new DataOutputStream(sock.getOutputStream());
			Scanner in = new Scanner(sock.getInputStream());
			out.writeBytes("GET~LOGS\n");
			while(in.hasNextLine()) {
				String line = in.nextLine();
				String[] splitLine = line.split(" ");
				//Filter out messages from the Worker class. There are a lot since NM connections are transient and we're polling NM's once a second.
				if(splitLine.length == 13 && splitLine[5].equals("Node") && splitLine[6].equals("Manager"))
					continue;
				System.out.println(line);
			}
			in.close();
			out.close();
			sock.close();
		} catch(Exception ex) {
			throw new Error(ex);
		}
	}

	public void addConnection(Connection conn) {
		this.connections.add(conn);
	}

	public List<Connection> getConnections() {
		return this.connections;
	}

	/**
	 * Terminates all AWS nodes in a cluster (that's 'contained' in the given EC2 client object).
	 * Checks IP addresses against those stored in the global connections arraylist. That list is populated
	 * when the cluster is initially created. This prevents nodes that aren't in the created cluster from being
	 * terminated.
	 * @param client
	 */
	public void terminateAllAWSNodes(AmazonEC2Client client) {
		//Make sure addr is NOT 127.0.0.1 before calling client.terminate.
		DescribeInstancesResult dir = client.describeInstances();
		for(Reservation r : dir.getReservations()) {
			for(Instance i : r.getInstances()) {
				//If the instance is "connected" to the admin and it's current state is 'running', terminate it.
				if(isConnectedToAdmin(i.getPublicIpAddress()) && i.getState().getCode() == 16) {
					TerminateInstancesRequest tir = new TerminateInstancesRequest().withInstanceIds(i.getInstanceId());
					TerminateInstancesResult tires = client.terminateInstances(tir);
					this.removeConnection(i.getPublicIpAddress());
				}
			}
		}
	}

	/**
	 * Terminates a single AWS instance node.
	 * @param addr
	 * @param client
	 */
	public void terminateAWSNode(String addr, AmazonEC2Client client) {
		DescribeInstancesResult dir = client.describeInstances();
		for(Reservation r : dir.getReservations()) {
			for(Instance i : r.getInstances()) {
				//If the instance is "connected" to the admin and it's current state is 'running', terminate it.
				if(i.getPublicIpAddress().equals(addr) && i.getState().getCode() == 16) {
					TerminateInstancesRequest tir = new TerminateInstancesRequest().withInstanceIds(i.getInstanceId());
					TerminateInstancesResult tires = client.terminateInstances(tir);
					this.removeConnection(i.getPublicIpAddress());
					return;
				}
			}
		}
	}

	/**
	 * Checks to see if a given IP address exists in the connections list (meaning it's part of a cluster).
	 * @param publicIpAddress
	 * @return
	 */
	private boolean isConnectedToAdmin(String publicIpAddress) {
		for(Connection conn : connections) 
			if(conn.addr.equals(publicIpAddress))
				return true;
		return false;
	}

	/**
	 * Sends a message to a node or node manager.
	 * @param conn
	 * @param command
	 * @throws UnknownHostException
	 * @throws IOException
	 */
	private void sendMessage(Connection conn, String command) throws UnknownHostException, IOException {
		Socket sock = new Socket(conn.addr, conn.port);
		DataOutputStream out = new DataOutputStream(sock.getOutputStream());
		out.writeBytes(command + "\r\n");
		out.flush();
		sock.close();
	}

	private void removeConnection(String ipAddress) {
		for(Iterator<Connection> it = connections.iterator(); it.hasNext(); ) { 
			Connection conn = it.next();
			if(conn.addr.equals(ipAddress)) 
				it.remove();
		}
	}

	public boolean jobIsComplete(Connection conn) throws UnknownHostException, IOException {
		Socket sock = new Socket(conn.addr, conn.port);
		DataOutputStream out = new DataOutputStream(sock.getOutputStream());
		Scanner in = new Scanner(sock.getInputStream());
		out.writeBytes("GET~STATUS\r\n");
		out.flush();
		String status = in.nextLine().trim();
		in.close();
		out.close();
		sock.close();
		return status.equalsIgnoreCase("complete");
	}
}
