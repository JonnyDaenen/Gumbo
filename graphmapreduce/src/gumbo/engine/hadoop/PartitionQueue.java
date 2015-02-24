/**
 * Created: 12 Feb 2015
 */
package gumbo.engine.hadoop;

import gumbo.compiler.calculations.CalculationUnit;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

/**
 * Represents a queue of partitions, coupled to real hadoop jobs.
 * When the job status is updated, the queue is adjusted accordingly and
 * new (depending) jobs are made available to execute.
 * 
 * @author Jonny Daenen
 *
 */
public class PartitionQueue {
	

	private static final Log LOG = LogFactory.getLog(PartitionQueue.class);
	
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
	public PartitionQueue(PartitionedCUGroup partitions) {
		this.partitions = partitions;
		queue = new HashSet<CalculationUnitGroup>();
		queue.addAll(partitions.getBottomUpList());
		active = new HashSet<CalculationUnitGroup>();
		ready = new HashSet<CalculationUnitGroup>();
		
		calc2firstjob = new DualHashBidiMap<CalculationUnitGroup, ControlledJob>();
		calc2secondjob = new DualHashBidiMap<CalculationUnitGroup, ControlledJob>();
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
		

//		LOG.info("queue " +  queue);
//		LOG.info("active " + active);
//		LOG.info("ready " +  ready);
		
		// for each job in active
		for (CalculationUnitGroup group: active) {
			// check if it is ready and mark it if so
			if (isReady(group)) {
				ready.add(group);
			}
		}
		
		// remove all ready jobs from the active list
		active.removeAll(ready);
		

		Set<CalculationUnitGroup> newlyActivated = new HashSet<>();
		// for each job in queue
		for (CalculationUnitGroup group: queue) {
		// check if dependencies are done
			if (dependenciesReady(group)) {

				LOG.info("Calculation group " + group + " is ready to be scheduled.");
				
				// make it active if so
				active.add(group);
				newlyActivated.add(group);
			}
		}
		
		// remove active jobs from the queue
		queue.removeAll(active);
		
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
		
		LOG.info("Dependencies: " + deps);
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
	private boolean isReady(CalculationUnitGroup group) {
		ControlledJob job1 = calc2firstjob.get(group);
		ControlledJob job2 = calc2secondjob.get(group);
		
		if (job1.isReady()) {
			LOG.info("Calculation group " + group + " round1 is ready.");
		}
		
		if (job2.isReady()) {
			LOG.info("Calculation group " + group + " round2 is ready.");
		}
		 
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
