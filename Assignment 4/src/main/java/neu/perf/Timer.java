package neu.perf;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * A simple class for recording execution time.
 * 
 * @author jan
 */
public class Timer {

	long start = -1;
	long stop = -1;

	/**
	 * Creates a new timer, initialized to the current time.
	 * 
	 * @author jan
	 */
	public Timer() {
		start = System.currentTimeMillis();
	}

	/**
	 * Stops the timer.
	 * 
	 * @author jan
	 */
	public void stop() {
		if (stop != -1) throw new Error("Attempting to reuse a timer");
		stop = System.currentTimeMillis();
	}

	public int seconds() {
		return (int) ((stop - start) / 1000);
	}

	/**
	 * Write the prefix text followed by a comma and the elapsed time in millis.
	 * 
	 * @author jan
	 * @param filename
	 *            the log file
	 * @param message
	 *            the prefix text
	 */
	public void log(String filename, String message) {
		if (stop - start < 15) Log.e("Warning below timer accuracy.");
		BufferedWriter out = null;
		try {
			FileWriter fstream = new FileWriter(filename, true);
			out = new BufferedWriter(fstream);
			out.write("\n" + message + "," + (stop - start));
		} catch (IOException e) {
			System.err.println("Error: " + e.getMessage());
		} finally {
			if (out != null) try {
				out.close();
			} catch (IOException e) {
				System.err.println("Error: " + e.getMessage());
			}
		}
	}
}
