package neu.perf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

/**
 * Logging error and normal output messages.
 * 
 * @author jan
 */
public class Log {

	static BufferedWriter errLog;
	static BufferedWriter outLog;

	static final boolean DEBUG = false;
	static final Timer time = new Timer();

	static {
		try {
			Date d = new Date();
			errLog = new BufferedWriter(new FileWriter(new File("/tmp/" + d + ".err.log")));
			outLog = new BufferedWriter(new FileWriter(new File("/tmp/" + d + ".out.log")));
			String s = "Run started at " + d.toString() + "\n";
			errLog.write(s);
			errLog.flush();
			outLog.write(s);
			outLog.flush();
		} catch (IOException e) {
			throw new Error(e);
		}
	}

	public static synchronized void e(String s) {
		try {
			errLog.write(s);
			errLog.newLine();
			if (DEBUG)
				System.out.println(s);
		} catch (IOException e) {
			throw new Error(e);
		}
	}

	public static synchronized void p(String s) {
		try {
			outLog.write(s);
			outLog.newLine();
			System.out.println(s);
		} catch (IOException e) {
			throw new Error(e);
		}
	}

	public static synchronized void p(int s) {
		p(Integer.toString(s));
	}

	public static synchronized void p(long s) {
		p(Long.toString(s));
	}

	public static synchronized void p(double s) {
		p(Double.toString(s));

	}

	public static void close() {
		try {
			time.stop();
			String s = "Ran in " + time.seconds() + " [sec]\n";
			e(s);
			outLog.close();
			errLog.close();
		} catch (IOException e) {
			throw new Error(e);
		}
	}
}
