package gumbo.engine.general.grouper;

import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.engine.general.grouper.policies.GroupingPolicy;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardAlgorithm;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class Grouper {
	

	private static final Log LOG = LogFactory.getLog(Grouper.class);
	
	protected Decomposer decomposer;
	protected GroupingPolicy policy;
	

	public Grouper(GroupingPolicy policy) {
		this.decomposer = new Decomposer();
		this.policy = policy;
	}

	/**
	 * Adds grouping information to each partition.
	 * 
	 * @param partitions
	 */
	public GroupedPartitionedCUGroup group(PartitionedCUGroup partitions){
		
		GroupedPartitionedCUGroup result = new GroupedPartitionedCUGroup(partitions);
		
		int numPart = partitions.getNumPartitions();
		// process each partition
		for (int level = 0; level < numPart; level++) {
			
			LOG.info("Grouping level " + level );
			
			CalculationUnitGroup partition = partitions.getPartition(level);
			LOG.info("Number of calculations: " + partition.size() );
			
			List<CalculationGroup> groupedSJ = group(partition);
			
			result.setGroup(level, groupedSJ);
			
		}
		
		return result;

	}

	/**
	 * Adds grouping to one partition.
	 * @param partition
	 * @return
	 */
	public List<CalculationGroup> group(CalculationUnitGroup partition) {
		
		// decompose
		CalculationGroup semijoins = decomposer.decompose(partition);
		
		LOG.info("Decomposition complete: " + semijoins );
		
		// apply grouping using the policy
		List<CalculationGroup> groupedSJ = policy.group(semijoins);
		
		LOG.info("Grouping complete: " + groupedSJ.size() + " groups" );
		
		return groupedSJ;
		
	}

}
