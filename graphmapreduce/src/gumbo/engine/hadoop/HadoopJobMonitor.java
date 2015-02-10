/**
 * Created: 05 Feb 2015
 */
package gumbo.engine.hadoop;

import gumbo.compiler.structures.MRPlan;

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
public class HadoopJobMonitor {


	private MRJob2HadoopConverter jobConverter;


	public HadoopJobMonitor() {
		jobConverter = new MRJob2HadoopConverter();
	}

	/**
	 * Executes a MR plan using Hadoop.
	 * @param plan a MR plan
	 */
	public void executePlan (MRPlan plan) {


		executeJobs(plan);
		cleanup();

	}

	/**
	 * 
	 */
	private void cleanup() {
		// TODO Auto-generated method stub

	}

	/**
	 * @param plan
	 */
	private void executeJobs(MRPlan plan) {
		
		// TODO implement
		
		// while not all completed
			// for each unstarted job
				// if previous job is done
				// get statistics from previous job
				// convert job to hadoop job using only files (no glob nor directories)
				// submit job
			// print status
			// sleep
		
		
		// print statistics
		
		// clean up in case of failure
		
		

	}




}
