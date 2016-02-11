package neu.perf;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * Submit a job for execution (possibly multiple times) and report the time
 * taken to run it. Note: time measurements are not very precise as they include
 * process creation, the expectation is that jobs run multiple seconds and that
 * JVM warm up is amortized. Arguments are obtained from the command line and
 * from a file.
 * 
 * @author Joe Sackett, @maintainer Jan Vitek
 */
final class App {
  private final Conf conf;
  private final Job job;

  App(Args args) throws Exception {
    conf = new Conf(args);
    if (conf.kind.equals("cloud")) initS3();
    job = Job.build(conf);
  }

  public static void main(String[] args) throws Exception {
    Logger.getRootLogger().setLevel(Level.OFF);
    App app = new App(new Args(args));
    for (int i = 0; i < app.conf.num; i++)
      app.job.run();
    app.job.finish();
    app.output(app.job.getResults(), new FileWriter(app.conf.results, true));
  }

  private void initS3() throws Exception {
    S3Service aws = new S3Service(conf.region);
    String it = conf.checkBucket;
    if (ok(it)) System.out.println("Bucket " + it + " exists " + aws.bucketExists(it));
    it = conf.checkInput;
    if (ok(it)) System.out.println("Folder " + it + " exists " + aws.folderExists(it));
    it = conf.checkLogs;
    if (ok(it)) System.out.println("Folder " + it + " exists " + aws.folderExists(it));
    it = conf.deleteOutput;
    if (ok(it)) System.out.println("Deleting " + it + " success " + aws.deleteFolder(it));
    it = conf.uploadJar;
    if (ok(it)) System.out.println("Uploading " + it + " success " + aws.uploadFile(conf.jar, it));
  }

  private boolean ok(String s) {
    return s != null && s.length() > 0;
  }

  private void output(Integer[] results, FileWriter fw) throws IOException {
    String brand = conf.name + "," + conf.kind;
    String res = "";
    for (Integer i : results)
      res += brand + "," + i + "\n";
    try {
      fw.write(res);
    } catch (IOException e) {
      throw new Error(e);
    }
    fw.close();
  }

  /**
   * This class runs an operating process, reading its standard and error output
   * in two parallel threads.
   */
  static class Executor {
    private ProcessBuilder builder; // the process to run

    Executor(ProcessBuilder builder) {
      this.builder = builder;
    }

    /**
     * Returns when the process is done. All output of the process is mirrored
     * to standard out.
     * 
     * @throws Exception
     */
    void execute() throws Exception {
      Process proc = builder.start();
      (new Thread(new Reader(new BufferedReader(new InputStreamReader(proc.getInputStream()))))).start();
      (new Thread(new Reader(new BufferedReader(new InputStreamReader(proc.getErrorStream()))))).start();
      proc.waitFor();
      if (proc.exitValue() > 0) throw new RuntimeException("Error in " + builder.toString());
    }

    /**
     * Reads a stream until dry.
     */
    private static class Reader implements Runnable {
      private BufferedReader bread;

      Reader(BufferedReader reader) {
        this.bread = reader;
      }

      public void run() {
        try {
          String line;
          while ((line = bread.readLine()) != null)
            Log.p(line);
          bread.close();
        } catch (IOException e) {
          throw new Error(e);
        }
      }
    }
  }

  /**
   * Hold those AWS options
   * 
   * @author Joe Sackett
   */
  static class Conf {
    final Integer num;
    final String results;
    final String jar;
    final String region;
    final String checkBucket;
    final String uploadJar;
    final String checkInput;
    final String checkLogs;
    final String deleteOutput;
    final String hadoopHome;
    final String clusterName;
    final String stepName;
    final String releaseLabel;
    final String logUri;
    final String serviceRole;
    final String jobFlowRole;
    final String ec2KeyName;
    final Integer instanceCount;
    final Boolean keepJobFlowAlive;
    final String masterInstance;
    final String slaveInstance;
    final String kind;
    final String name;
    final String main;
    final String input;
    final String output;
    final String[] arguments;
    final PropertiesConfiguration pc = new PropertiesConfiguration();
    final Args args;

    Conf(Args args) {
      this.args = args;
      try {
        pc.load(args.getOpt("properties", "perf.txt"));
      } catch (ConfigurationException e) {
        throw new Error(e);
      }
      this.num = i("num", 1);
      this.results = s("results", "results.csv");
      this.jar = s("jar", "job.jar");
      this.hadoopHome = s("hadoop.home");
      // AWS S3 Initialization
      this.region = s("region", "us-east-1");
      this.checkBucket = s("check.bucket");
      this.checkInput = s("check.input");
      this.checkLogs = s("check.logs");
      this.deleteOutput = s("delete.output");
      this.uploadJar = s("upload.jar");
      // AWS EMR Configuration
      this.clusterName = s("cluster.name", "My 6240 cluster");
      this.stepName = s("step.name", "My step");
      this.releaseLabel = s("release.label", "emr-4.2.0");
      this.logUri = s("log.uri");
      this.serviceRole = s("service.role", "EMR_DefaultRole");
      this.jobFlowRole = s("job.flow.role", "EMR_EC2_DefaultRole");
      this.ec2KeyName = s("ec2.key.name");
      this.instanceCount = i("instance.count", 3);
      this.keepJobFlowAlive = b("keep.job.flow.alive");
      this.masterInstance = s("master.instance.type", "m3.xlarge");
      this.slaveInstance = s("slave.instance.type", "m3.xlarge");
      this.kind = s("kind", "plain");
      this.name = s("name", "job");
      this.main = s("main");
      this.input = s("input", "");
      this.output = s("output", "");
      this.arguments = sv("arguments");
    }

    String s(String name, String defv) {
      String a = args.getOpt(name);
      String p = pc.getString(name);
      return a != null ? a : (p != null ? p : defv);
    }

    String s(String name) {
      return s(name, null);
    }

    int i(String name, int defv) {
      String res = s(name);
      return res != null ? Integer.parseInt(res) : defv;
    }

    boolean b(String name) {
      return args.getFlag(name) || pc.getBoolean(name, false);
    }

    String[] sv(String name) {
      String v = s(name);
      return v == null ? new String[0] : v.split(" ");
    }
  }
}
