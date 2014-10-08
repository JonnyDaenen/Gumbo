/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.executor.hadoop;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import mapreduce.guardedfragment.planner.structures.MRJob;
import mapreduce.guardedfragment.planner.structures.MRPlan;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

/**
 * Executes an MR-plan on Hadoop.
 * 
 * @author Jonny Daenen
 * 
 */
public class HadoopExecutor {

	private static final Log LOG = LogFactory.getLog(HadoopExecutor.class);

	private static final int REFRESH_WAIT = 500; // ms
	protected boolean deleteTmpDirs;
	private MRJob2HadoopConverter jobConverter;

	/**
	 * 
	 */
	public HadoopExecutor() {
		jobConverter = new MRJob2HadoopConverter();
		deleteTmpDirs = true;
	}

	public void execute(MRPlan plan) {

		try {
			// convert MRplan to Hadoop framework
			Collection<ControlledJob> hadoopJobs = convertJobs(plan);

			// execute in Hadoop
			executePlan(plan, hadoopJobs); // TODO catch success flag

			// cleanup
			cleanUp(plan);

			LOG.info("Done.");

		} catch (Exception e) {
			// TODO do something
			e.printStackTrace();
		}
	}

	/**
	 * Removes temp directories if required.
	 * 
	 * @param plan
	 */
	private void cleanUp(MRPlan plan) {
		// remove the temp dirs
		if (plan.deleteTmpDirsEnabled()) {
			LOG.info("Removing non-output data...");
			deleteTmpDirs(plan);
		}

	}

	/**
	 * Executes a set of jobs on the current Hadoop system.
	 * 
	 * @param jobs
	 *            the set of ControlledJobs to execute on Hadoop
	 * @param plan
	 *            the MRPlan containing the jobs to convert
	 * @return true if success, false otherwise
	 */
	private boolean executePlan(MRPlan plan, Collection<ControlledJob> jobs) throws Exception {
		boolean success = false;
		String name = plan.getName();

		// 1. create JobControl
		JobControl jc = new JobControl(name);

		// 2. we add all the controlled jobs to the job control
		// note that this can also be done in 1 go by using a collection
		jc.addJobCollection(jobs);

		// 3. we execute the jobcontrol in a Thread
		Thread workflowThread = new Thread(jc, "Fronjo-Workflow-Thread_" + name);
		workflowThread.setDaemon(true); // will not avoid JVM to shutdown

		LOG.info("Starting Job-control thread: " + workflowThread.getName());
		workflowThread.start();

		// 4. we wait for it to complete
		LOG.info("Waiting for thread to complete: " + workflowThread.getName());
		while (!jc.allFinished()) {
			printStatus(jc);
			printProgress(jc);
			Thread.sleep(REFRESH_WAIT);


		}

		// 5. clean up in case of failure

		if (jc.getFailedJobList().size() > 0) {
			LOG.error(jc.getFailedJobList().size() + " jobs failed!");

			for (ControlledJob job : jc.getFailedJobList()) {
				LOG.error("\t" + job.getJobName() + " failed.");
			}
		} else {
			success = true;
			LOG.info("SUCCESS: all jobs (" + jc.getSuccessfulJobList().size() + ") completed!");
			// move output to output directory
			LOG.info("Copying output data...");
			assembleOutput(plan);
			
			for (ControlledJob job : jc.getSuccessfulJobList()) {
				LOG.error("profile: " +  job.getJob().getProfileParams());
				LOG.error("time: " +  (job.getJob().getFinishTime() - job.getJob().getStartTime()));
			}

		}

		return success;
	}

	/**
	 * @param jc
	 */
	private void printProgress(JobControl jc) {
//		try {
//			for (ControlledJob job : jc.getRunningJobList()) {
//				LOG.debug(job.getJob().mapProgress());
//			}
//		} catch (IOException  e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}

	}

	/**
	 * Converts each MRJob to a Hadoop Map-Reduce ControlledJob ands sets
	 * dependencies between them.
	 * 
	 * @param plan
	 *            the MRPlan containing the jobs to convert
	 * @return a set of ControlledJobs, corresponding to the MRJobs that are
	 *         given in the plan
	 * 
	 */
	private Collection<ControlledJob> convertJobs(MRPlan plan) {
		HashMap<MRJob, ControlledJob> map = new HashMap<MRJob, ControlledJob>();

		// convert and create 1-1 mapping
		for (MRJob job : plan.getJobs()) {
			ControlledJob cjob = jobConverter.convert(job);
			map.put(job, cjob);
		}

		// set dependencies
		for (MRJob job : map.keySet()) {
			ControlledJob cjob = map.get(job);
			for (MRJob jobDependency : job.getDependencies()) {
				cjob.addDependingJob(map.get(jobDependency));
			}
		}

		return map.values();

	}

	/**
	 * @param plan
	 * 
	 */
	private void assembleOutput(MRPlan plan) {

		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			for (Path outDir : plan.getOutdirs()) {
				Path newLocation = plan.getOutputFolder().suffix(Path.SEPARATOR + outDir.getName());
				FileUtil.copy(fs, outDir, fs, newLocation, false, conf);
			}
		} catch (IOException e) {
			LOG.error("WARNING: problem assembling output!");
			e.printStackTrace();
		}

	}

	/**
	 * Delete all temporary dirs from the file system.
	 * 
	 * @throws IOException
	 */
	private void deleteTmpDirs(MRPlan plan) {
		FileSystem fs;

		// delete temp dirs
		try {
			fs = FileSystem.get(new Configuration());

			for (Path p : plan.getTmpDirs()) {
				System.out.println("Checking: " + p);
				if (fs.exists(p)) {
					fs.delete(p, true); // delete recursive
				}
			}
		} catch (IOException e) {
			LOG.error("WARNING: problem deleting temporary folders!");
			e.printStackTrace();
		}

		// delete scratch dir
		try {
			fs = FileSystem.get(new Configuration());

			for (Path p : plan.getTmpDirs()) {
				// FIXME is this ok, it seems identical to the code above??
				System.out.println("Checking: " + p);
				if (fs.exists(p)) {
					fs.delete(p, true); // delete recursive
				}
			}
		} catch (IOException e) {
			System.err.println("WARNING: problem deleting temporary folders!");
			e.printStackTrace();
		}

	}

	/* GETTERS & SETTERS */

	/**
	 * @param job
	 * @return
	 */
	private String jobsummary(ControlledJob job) {
		String output = "";
		// jobid will be unassigned in beginning
		output += "Job: " + job.getJobName();

		output += System.getProperty("line.separator");
		Path[] inputs1 = FileInputFormat.getInputPaths(new JobConf(job.getJob().getConfiguration()));
		List<Path> inputs = Arrays.asList(inputs1);
		output += "Input paths: " + inputs;
		output += System.getProperty("line.separator");

		output += "Output path: " + FileOutputFormat.getOutputPath(new JobConf(job.getJob().getConfiguration()));
		output += System.getProperty("line.separator");

		if (job.getDependentJobs() != null) {
			String deps = "";
			for (ControlledJob dep : job.getDependentJobs()) {
				deps += "," + dep.getJobName();
			}
			output += "Depending on: {" + deps.substring(1) + "}";
		}

		output += System.getProperty("line.separator");

		return output;
	}

	/**
	 * @param jc
	 *            the jobcontrol
	 */
	private void printStatus(JobControl jc) {

		LOG.debug("Ready: " + jc.getReadyJobsList());
		LOG.debug("Failed: " + jc.getFailedJobList());
		LOG.debug("Running: " + jc.getRunningJobList());
		LOG.debug("Success: " + jc.getSuccessfulJobList());
		LOG.debug("Waiting: " + jc.getWaitingJobList());
	}

}
