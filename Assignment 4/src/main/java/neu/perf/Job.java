package neu.perf;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import org.apache.commons.lang.ArrayUtils;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.model.ClusterState;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.ListStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.ListStepsResult;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepSummary;

/**
 * A job to be submitted either locally or to AWS.
 * 
 * @author Joe Sackett, maintainer Jan Vitek, @maintainer Surekha Jadhwani
 */
public abstract class Job {
  App.Conf conf; // the configuration of AWS
  Vector<Integer> seconds = new Vector<Integer>(); // execution time in seconds

  public static Job build(App.Conf conf) {
    if (conf.kind.equals("single-thread") || conf.kind.equals("multithread")) 
    	return new Plain(conf);
    else if (conf.kind.equals("local")) 
    	return new Local(conf);
    else if (conf.kind.equals("cloud")) 
    	return new Cloud(conf);
    else throw new Error("Specify job kind");
  }

  Job(App.Conf conf) {
    this.conf = conf;
  }

  public abstract void run() throws Exception;

  public void finish() {}

  public Integer[] getResults() {
    return seconds.toArray(new Integer[0]);
  }

  public int timeExec(ProcessBuilder builder) throws Exception {
    App.Executor proc = new App.Executor(builder);
    Timer time = new Timer();
    proc.execute();
    time.stop();
    return time.seconds();
  }

  String[] concat(String[] preArgs, String[] args) {
    return (String[]) ArrayUtils.addAll(preArgs, args);
  }

  /**
   * A Java task running locally without Hadoop.
   */
  static class Plain extends Job {

    Plain(App.Conf conf) {
      super(conf);
    }

    @Override
    public void run() throws Exception {
      seconds.add(timeExec(new ProcessBuilder(concat(new String[] { "java", "-cp", conf.jar, conf.main}, conf.arguments)))); 
      		
    }
  }

  /**
   * A local pseudo-distributed task running with Hadoop.
   */
  static class Local extends Job {

    Local(App.Conf conf) {
      super(conf);
    }

    @Override
    public void run() throws Exception {
    	ProcessBuilder pb = new ProcessBuilder(concat(new String[] { conf.hadoopHome + "/bin/hadoop", "jar", conf.jar, conf.main,
          "-input=" + conf.input, "-output=" + conf.output }, conf.arguments));
      deleteDirectory(new File(conf.output));
      seconds.add(timeExec(pb));
    }

    private static boolean deleteDirectory(File dir) {
      if (!dir.exists() || !dir.isDirectory()) return false;
      File[] files = dir.listFiles();
      if (files != null) for (File file : files)
        if (file.isDirectory()) deleteDirectory(file);
        else file.delete();
      return (dir.delete());
    }
  }

  /**
   * A task running on AWS.
   */
  static class Cloud extends Job {

    AmazonElasticMapReduce client = new AmazonElasticMapReduceClient(new ProfileCredentialsProvider().getCredentials());
    List<StepConfig> steps = new LinkedList<StepConfig>();
    int num;
    String clusterId;

    Cloud(App.Conf conf) {
      super(conf);
    }

    @Override
    public void run() throws Exception { // Hadoop program step.
      HadoopJarStepConfig hc = new HadoopJarStepConfig().withJar(conf.jar).withMainClass(conf.main)
          .withArgs(concat(new String[] { "-input=" + conf.input, "-output=" + conf.output }, conf.arguments));
      steps.add(new StepConfig(conf.stepName + " " + num, hc));
      num++;
    }

    @Override
    public void finish() {
      RunJobFlowRequest request = new RunJobFlowRequest().withName(conf.clusterName).withReleaseLabel(conf.releaseLabel)
          .withSteps(steps.toArray(new StepConfig[0])).withLogUri(conf.logUri).withServiceRole(conf.serviceRole)
          .withJobFlowRole(conf.jobFlowRole)
          .withInstances(new JobFlowInstancesConfig().withEc2KeyName(conf.ec2KeyName).withInstanceCount(conf.instanceCount)
              .withKeepJobFlowAliveWhenNoSteps(conf.keepJobFlowAlive).withMasterInstanceType(conf.masterInstance)
              .withSlaveInstanceType(conf.slaveInstance));

      RunJobFlowResult res = client.runJobFlow(request);
      clusterId = res.getJobFlowId();

      DescribeClusterRequest clusterReq = new DescribeClusterRequest().withClusterId(clusterId);

      while (true) {
        DescribeClusterResult cres = client.describeCluster(clusterReq);
        String state = cres.getCluster().getStatus().getState();
        if (state.equals(ClusterState.TERMINATED.name()) || state.equals(ClusterState.TERMINATED_WITH_ERRORS.name())
            || state.equals(ClusterState.TERMINATING.name())) {
          break;
        }
        try {
          Thread.sleep(10 * 1000);
        } catch (InterruptedException e) {}
      }
    }

    @Override
    public Integer[] getResults() {
      ListStepsRequest req = new ListStepsRequest().withClusterId(clusterId);
      ListStepsResult res = client.listSteps(req);
      for (StepSummary sum : res.getSteps()) {
        long time = sum.getStatus().getTimeline().getEndDateTime().getTime()
            - sum.getStatus().getTimeline().getStartDateTime().getTime();
        seconds.add((int) (time / 1000));
      }
      return seconds.toArray(new Integer[0]);
    }
  }
}
