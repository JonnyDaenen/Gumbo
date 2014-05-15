package mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mapreduce.data.RelationSchema;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

/**
 * Provides a way to execute a set of MR jobs that are possibly dependent. All
 * jobs are executed in a seperate thread, which is polled for completion.
 * Input, output, scratch and temporary directories are to be specified. Scratch
 * and temp folders are removed after completion, unless suppressed with the
 * supplied preference.
 * 
 * The output and scratch folders should belong to one plan. The temp folders
 * should be located inside the scratch folder, but can later be used to provide
 * a more early temp folder deletion mechanism.
 * 
 * A summary of the plan is placed inside the output folder.
 * 
 * @author Jonny Daenen
 * 
 */
public class MRPlan {

	private static final Log LOG = LogFactory.getLog(MRPlan.class);
	
	private static final int REFRESH_WAIT = 500; //ms

	protected String name = "FronjoPlan"; // FUTURE change

	protected Path inputFolder; // FUTURE I think this should be a set
	protected Path outputFolder;
	protected Path scratchFolder;

	protected Set<ControlledJob> jobs;
	protected Set<Path> tempdirs;
	protected Set<Path> outdirs;

	protected Set<RelationSchema> relations; // TODO add to description

	boolean deleteTmpDirs;

	public MRPlan() {
		jobs = new HashSet<ControlledJob>();
		tempdirs = new HashSet<Path>();
		outdirs = new HashSet<Path>();
		deleteTmpDirs = true;
	}

	/**
	 * Add a job.
	 * 
	 * @param job
	 *            the job to add
	 */
	public void addJob(ControlledJob job) {
		jobs.add(job);
	}

	public void addAllJobs(Collection<ControlledJob> jobs) {
		this.jobs.addAll(jobs);
	}

	public void addTempDir(Path tmpDir) {
		tempdirs.add(tmpDir);
	}

	public void addTempDirs(Collection<Path> tmpDirs) {
		tempdirs.addAll(tmpDirs);

	}

	public void addOutDirs(Collection<Path> outDirs) {
		outdirs.addAll(outDirs);

	}

	/**
	 * Run the entire MapReduce plan.
	 * 
	 * @return true when the plan has successfully been executed, false
	 *         otherwise
	 */
	public boolean execute() throws Exception {

		boolean success = false;

		try {

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
				//printStatus(jc);
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
				assembleOutput();
			}

		} finally {
			// remove the temp dirs
			if (deleteTmpDirs) {
				LOG.info("Removing non-output data...");
				deleteTmpDirs();
			}

			LOG.info("Done.");
		}

		return success;
	}

	/**
	 * @param jc the jobcontrol
	 */
	private void printStatus(JobControl jc) {
		// TODO Auto-generated method stub
		
		LOG.info("Ready: " + jc.getReadyJobsList());
		LOG.info("Failed: " + jc.getFailedJobList());
		LOG.info("Running: " + jc.getRunningJobList());
		LOG.info("Success: " + jc.getSuccessfulJobList());
		LOG.info("Waiting: " + jc.getWaitingJobList());
	}

	/**
	 * 
	 */
	private void assembleOutput() {
		
		Configuration conf = new Configuration();
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			for (Path outDir : outdirs) {
				Path newLocation = outputFolder.suffix(Path.SEPARATOR + outDir.getName());
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
	private void deleteTmpDirs() {
		FileSystem fs;

		// delete temp dirs
		try {
			fs = FileSystem.get(new Configuration());

			for (Path p : tempdirs) {
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

			for (Path p : tempdirs) {
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

	/**
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		String output = "";

		// TODO add date

		// name
		output += System.getProperty("line.separator");
		output += "MR-plan:";
		output += System.getProperty("line.separator");
		output += name;
		output += System.getProperty("line.separator");

		// jobs
		output += System.getProperty("line.separator");
		output += "Jobs:";
		output += System.getProperty("line.separator");
		output += "-----";
		output += System.getProperty("line.separator");

		for (ControlledJob job : jobs) {
			output += jobsummary(job);
		}

		// folders
		output += System.getProperty("line.separator");
		output += "Folders:";
		output += System.getProperty("line.separator");
		output += "-------";
		output += System.getProperty("line.separator");

		output += "input: " + inputFolder;
		output += System.getProperty("line.separator");
		output += "output: " + outputFolder;
		output += System.getProperty("line.separator");
		output += "scratch: " + scratchFolder;
		output += System.getProperty("line.separator");
		
		output += "Output-dirs: ";
		output += System.getProperty("line.separator");
		for (Path dir : outdirs) {
			output += "\t" + dir.toString();
		}
		output += System.getProperty("line.separator");
		
		output += "Temp-dirs: ";
		output += System.getProperty("line.separator");
		for (Path dir : tempdirs) {
			output += "\t" + dir.toString();
		}
		output += System.getProperty("line.separator");

		return output;
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

	public Path getInputFolder() {
		return inputFolder;
	}

	public void setInputFolder(Path inputFolder) {
		this.inputFolder = inputFolder;
	}

	public Path getOutputFolder() {
		return outputFolder;
	}

	public void setOutputFolder(Path outputFolder) {
		this.outputFolder = outputFolder;
	}

	public Path getScratchFolder() {
		return scratchFolder;
	}

	public void setScratchFolder(Path scratchFolder) {
		this.scratchFolder = scratchFolder;
	}

	/**
	 * Toggles deletion of tmp dirs after execution. When enabled, they are also
	 * deleted when something has gone wrong.
	 * 
	 * @param deleteTmpDirs
	 */
	public void setDeleteTmpDirs(boolean deleteTmpDirs) {
		this.deleteTmpDirs = deleteTmpDirs;
	}

}
