/**
 * Created: 12 Feb 2015
 */
package gumbo.engine.hadoop;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.collections4.BidiMap;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;

/**
 * Represents a queue of partitions, coupled to real hadoop jobs.
 * When the job status is updated, the queue is adjusted accordingly and
 * new (depending) jobs are made available to execute.
 * 
 * @author Jonny Daenen
 *
 */
public class PartitionQueue {
	
	PartitionedCUGroup partitions;
	
	// job monitoring sets
	Set<CalculationUnitGroup> queue;
	Set<CalculationUnitGroup> active;
	Set<CalculationUnitGroup> ready;
	
	// mapping
	BidiMap<CalculationUnitGroup,ControlledJob> calc2firstjob;
	BidiMap<CalculationUnitGroup,ControlledJob> calc2secondjob;


	/**
	 * Creates and initializes a queue based on a partitioned set of {@link CalculationUnit}s.
	 */
	PartitionQueue(PartitionedCUGroup partitions) {
		this.partitions = partitions;
	}

	/**
	 * @return <code>true</code> if there are no partitions left to process, <code>false</code> otherwise.
	 */
	public boolean isEmpty() {
		return queue.isEmpty();
	}

	/**
	 * Removes ready partitions from the queue
	 * and returns the newly activated ones.
	 * 
	 * @return the set of newly activated partitions
	 */
	public Set<CalculationUnitGroup> updateStatus() {
		
		Set<CalculationUnitGroup> newlyActivated = new HashSet<>();
		// for each job in active
		for (CalculationUnitGroup job : active) {
			// check if it is ready and mark it if so
			if (isReady(job)) {
				active.remove(job);
				ready.add(job);
			}
		}
		
		// for each job in queue
		for (CalculationUnitGroup job: queue) {
		// check if dependencies are done
			if (dependenciesReady(job)) {
				// make it active if so
				queue.remove(job);
				active.add(job);
				newlyActivated.add(job);
			}
		}
		
		return newlyActivated;
	}

	/**
	 * Checks whether all dependencies are finished.
	 * 
	 * @param job
	 * 
	 * @return <code>true</code> if all dependcies are done, <code>false</code> otherwise
	 */
	private boolean dependenciesReady(CalculationUnitGroup cug) {
		
		// get dependencies
		
		Collection<CalculationUnitGroup> deps = partitions.getDependentPartitions(cug);
		
		// check their jobs
		boolean depsReady = true;
		for (CalculationUnitGroup dep : deps) {
			depsReady = depsReady &&  ready.contains(dep);
		}
		
		
		return depsReady;
	}

	/**
	 * Checks whether all dependencies are finished
	 * 
	 * @param job a job
	 * @param jobcontrol the jobcontrol
	 * 
	 * @return <code>true</code> if all dependcies are done, <code>false</code> otherwise
	 */
	private boolean isReady(CalculationUnitGroup job) {
		ControlledJob job1 = calc2firstjob.get(job);
		ControlledJob job2 = calc2secondjob.get(job);
		 
		return job1.isReady() && job2.isReady();
	}

	/**
	 * Connects jobs to a created partition.
	 * 
	 * @param partition a partition
	 * @param jobs hadoop jobs for the partitions
	 * 
	 */
	public void addJobs(CalculationUnitGroup partition, ControlledJob job1, ControlledJob job2) {
		calc2firstjob.put(partition, job1);
		calc2secondjob.put(partition, job2);
	}

}
