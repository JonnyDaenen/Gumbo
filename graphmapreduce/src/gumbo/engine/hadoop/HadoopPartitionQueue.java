/**
 * Created: 12 Feb 2015
 */
package gumbo.engine.hadoop;

import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.engine.general.PartitionQueue;

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
public class HadoopPartitionQueue extends PartitionQueue {
	

	private static final Log LOG = LogFactory.getLog(HadoopPartitionQueue.class);
	
	
	// mapping
	BidiMap<CalculationUnitGroup,ControlledJob> calc2firstjob;
	BidiMap<CalculationUnitGroup,ControlledJob> calc2secondjob;


	public HadoopPartitionQueue(PartitionedCUGroup partitions) {
		super(partitions);
		calc2firstjob = new DualHashBidiMap<CalculationUnitGroup, ControlledJob>();
		calc2secondjob = new DualHashBidiMap<CalculationUnitGroup, ControlledJob>();
	}



	/**
	 * Checks whether all dependencies are finished
	 * 
	 * @param job a job
	 * @param jobcontrol the jobcontrol
	 * 
	 * @return <code>true</code> if all dependcies are done, <code>false</code> otherwise
	 */
	protected boolean isReady(CalculationUnitGroup group) {
		ControlledJob job1 = calc2firstjob.get(group);
		ControlledJob job2 = calc2secondjob.get(group);
		
		if (job1.isReady()) {
			LOG.info("Calculation group " + group + " round1 is ready.");
		}
		
		if (job2.isReady()) {
			LOG.info("Calculation group " + group + " round2 is ready.");
		}
		 
		return job1.isCompleted() && job2.isCompleted();
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
