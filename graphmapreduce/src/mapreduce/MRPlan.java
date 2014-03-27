package mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class MRPlan extends Configured {

	protected String name = "FonjoAlphaPlan"; // FUTURE change
	protected String inputFolder; // FUTURE I think this should be a set
	protected String outputFolder; // FUTURE I think this should be a set
	protected Set<ControlledJob> jobs;
	protected Set<Path> tempdirs;

	public MRPlan() {
		jobs = new HashSet<ControlledJob>();
		tempdirs = new HashSet<Path>();
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

	public void addTempDir(String tmpDir) {
		tempdirs.add(new Path(tmpDir));
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
			JobControl jc = new JobControl(name); // TODO name

			// 2. we add all the controlled jobs to the job control
			// note that this can also be done in 1 go by using a collection
			jc.addJobCollection(jobs);

			// 3. we execute the jobcontrol in a Thread
			Thread workflowThread = new Thread(jc, "Fronjo-Workflow-Thread"); // TODO change
			workflowThread.setDaemon(true); // TODO what's this?
			workflowThread.start();

			// 4. we wait for it to complete
			while (!jc.allFinished()) {
				Thread.sleep(500);
			}

			// 5. clean up in case of failure

			if (jc.getFailedJobList().size() > 0) {
				System.err.println(jc.getFailedJobList().size() + " jobs failed!");

				for (ControlledJob job : jc.getFailedJobList()) {
					System.err.println("\t" + job.getJobName() + " failed.");
				}
			} else {
				success = true;
				System.err.println("SUCCESS: all jobs (" + jc.getSuccessfulJobList().size() + ") completed!");
			}

		} finally {
			// remove the temp dirs
			deleteTmpDirs();
		}

		return success;
	}

	/**
	 * Delete all temporary dirs from the file system
	 * 
	 * @throws IOException
	 */
	private void deleteTmpDirs() {
		FileSystem fs;
		try {
			fs = FileSystem.get(getConf());

			for (Path p : tempdirs) {
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
			// jobid will be unassigned in beginning
			output += "Job: " + job.getJobName();

			String deps = "";
			if (job.getDependentJobs() != null) {
				for (ControlledJob dep : job.getDependentJobs()) {
					deps += "," + dep.getJobName();
				}
				output += "Depending on: {" + deps.substring(1) + "}";
			}
			
			output += System.getProperty("line.separator");
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
		
		output += "Temp-dirs: ";
		output += System.getProperty("line.separator");
		for (Path dir : tempdirs) {
			output += "\t" + dir.toString();
		}

		return output;
	}

	/* GETTERS & SETTERS */

	public String getInputFolder() {
		return inputFolder;
	}

	public void setInputFolder(String inputFolder) {
		this.inputFolder = inputFolder;
	}

	public String getOutputFolder() {
		return outputFolder;
	}

	public void setOutputFolder(String outputFolder) {
		this.outputFolder = outputFolder;
	}

}
