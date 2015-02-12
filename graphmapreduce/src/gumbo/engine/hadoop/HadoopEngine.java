/**
 * Created: 05 Feb 2015
 */
package gumbo.engine.hadoop;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import gumbo.compiler.GumboPlan;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.compiler.structures.MRPlan;
import gumbo.engine.ExecutionException;
import gumbo.engine.hadoop.converter.GumboHadoopConverter;
import gumbo.engine.hadoop.converter.GumboHadoopConverter.ConversionException;
import gumbo.engine.hadoop.settings.ExecutorSettings;

/**
 * 
 * Runs a set of  jobs, which converted from abstract jobs to hadoop jobs at runtime.
 * This runtime creation allows for just-in-time estimation of input characteristics, 
 * which can be used to estimate the number of reducers.
 * 
 * 
 * @author Jonny Daenen
 *
 */
public class HadoopEngine {


	private static final long REFRESH_WAIT = 500; // FUTURE increase to 5000ms
	private static final Log LOG = LogFactory.getLog(HadoopEngine.class);


	public HadoopEngine() {
	}

	/**
	 * Executes a MR plan using Hadoop.
	 * @param plan a MR plan
	 * @throws ExecutionException 
	 */
	public void executePlan (GumboPlan plan) throws ExecutionException {

		GumboHadoopConverter jobConverter = new GumboHadoopConverter(plan.getName(), plan.getFileMapping(), new ExecutorSettings());

		executeJobs(plan, jobConverter);
		cleanup();

	}



	private void cleanup() {
		// TODO Auto-generated method stub

	}


	/**
	 * Executes a Gumbo Query and returns whether the execution was successful.
	 * Partitions are converted to hadoop {@link ControlledJob}s and fed to a {@link JobControl} instance
	 * one by one, to ensure that runtime adjustments can be made. Adjustments include for example the
	 * estimation of the number of reduce jobs, based on the input size.
	 * 
	 * @param plan a Gumbo plan
	 * @param jobConverter a converter
	 * 
	 * @return <code>true</code> when all jobs have finished successfully, <code>false</code> otherwise
	 * 
	 * @throws ExecutionException when the execution of the plan fails
	 */
	private boolean executeJobs(GumboPlan plan, GumboHadoopConverter jobConverter) throws ExecutionException {

		boolean success = false;

		try {

		// 1. create JobControl
		LOG.info("Creating Job Control for: " + plan.getName());
		JobControl jc = new JobControl(plan.getName());

		// create mapping for jobs and partitions
		BidiMap<CalculationUnitGroup,ControlledJob> calc2job = new DualHashBidiMap<>();
		BidiMap<ControlledJob, CalculationUnitGroup> job2calc = calc2job.inverseBidiMap();

		

		// 2. we execute the jobcontrol in a Thread
		Thread workflowThread = new Thread(jc, "Gumbo-Workflow-Thread_" + plan.getName());
		workflowThread.setDaemon(true); // will not avoid JVM to shutdown
		LOG.info("Starting Job-control thread: " + workflowThread.getName());
		workflowThread.start();

		// 3. execute jobs
		
		// for each partition from bottom to top
		PartitionQueue queue = new PartitionQueue(plan.getPartitions());
		
		LOG.info("Processing partition queue.");
		// while not all completed
		while (!queue.isEmpty()) {

			// update states
			Set<CalculationUnitGroup> newPartitions = queue.updateStatus();

			// for each job for which the dependencies are done
			// (and whose input files are now available for analysis)
			for (CalculationUnitGroup partition: newPartitions) {

				// convert job to hadoop job using only files 
				// TODO #core (no glob nor directories)
				List<ControlledJob> jobs = jobConverter.convert(partition);

				// add all the controlled jobs to the job control
				jc.addJobCollection(jobs);
				
				// update the queue
				queue.addJobs(partition, jobs.get(0), jobs.get(1));
			
			}

			// sleep
			Thread.sleep(REFRESH_WAIT);
		}
		LOG.info("Partition queue exhausted.");

		jc.stop();
		
		// 4. remove intermediate data

		// TODO implement removal of intermediate data

		// 5. report outcome
		if (jc.getFailedJobList().size() > 0) {
			LOG.error(jc.getFailedJobList().size() + " jobs failed!");

			for (ControlledJob job : jc.getFailedJobList()) {
				LOG.error("\t" + job.getJobName() + " failed.");
			}
			
			// TODO print out relations that failed
		} else {
			success = true;
			LOG.info("SUCCESS: all jobs (" + jc.getSuccessfulJobList().size() + ") completed!");
			
			// move output to output directory
			LOG.info("Copying output data...");

			for (ControlledJob job : jc.getSuccessfulJobList()) {
				LOG.error("profile: " +  job.getJob().getProfileParams());
				LOG.error("time: " +  (job.getJob().getFinishTime() - job.getJob().getStartTime()));
			}

		}
		
		} catch (ConversionException | IOException | InterruptedException e) {
			LOG.error("Exception during Gumbo plan execution: " + e.getMessage());
			throw new ExecutionException("Execution failed: " + e.getMessage(), e);
		}
		return success;

	}




}
