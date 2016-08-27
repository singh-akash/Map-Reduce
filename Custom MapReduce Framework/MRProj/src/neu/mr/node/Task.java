package neu.mr.node;

import java.io.IOException;
import java.io.PrintStream;
import java.util.List;

/**
 * Represents a Task managed by Node Manager.
 * @author Joe Sackett
 */
public class Task extends Server {

	public Task(int adminPortNum, String taskName) {	
		super(adminPortNum, new TaskServerStrategy(taskName));
	}

	/** Implementation for general Task. */
	private static class TaskServerStrategy extends AbstractServerTransient {
		private final String taskName;
		
		public TaskServerStrategy(String taskName) {
			this.taskName = taskName;
		}
		
		/** Echo name specific for this task. */
		@Override
		public String getTypeName() {
			return taskName;
		}
		
		/** Process request string & delegate to handler functions. */
		@Override
		protected void handleRequest(List<String> fullRequest, PrintStream writer, Server server) throws IOException {
			// Parse & validate request.
			String request = fullRequest.get(0);
			
			if ("shutdown".equalsIgnoreCase(request) || "kill".equalsIgnoreCase(request)) {
				server.shutdown();
			}
			else {
				System.out.println("Command not recognized.");
			}
	    }
	}
}
