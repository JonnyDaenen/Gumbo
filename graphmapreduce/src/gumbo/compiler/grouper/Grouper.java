package gumbo.compiler.grouper;

import java.util.List;
import java.util.Set;

import gumbo.compiler.grouper.policies.GroupingPolicy;
import gumbo.compiler.linker.CalculationUnitGroup;
import gumbo.compiler.partitioner.PartitionedCUGroup;


public class Grouper {
	
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
			
			CalculationUnitGroup partition = partitions.getPartition(level);
			List<Set<GuardedSemiJoinCalculation>> groupedSJ = getGrouping(partition);
			
			result.setGroup(level, groupedSJ);
			
		}
		
		return result;

	}

	/**
	 * Adds grouping to one partition.
	 * @param partition
	 * @return
	 */
	private List<Set<GuardedSemiJoinCalculation>> getGrouping(CalculationUnitGroup partition) {
		
		// decompose
		Set<GuardedSemiJoinCalculation> semijoins = decomposer.decompose(partition);
		
		// apply grouping using the policy
		List<Set<GuardedSemiJoinCalculation>> groupedSJ = policy.group(semijoins);
		
		return groupedSJ;
		
	}

}
