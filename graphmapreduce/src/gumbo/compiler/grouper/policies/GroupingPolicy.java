package gumbo.compiler.grouper.policies;

import gumbo.compiler.grouper.GuardedSemiJoinCalculation;
import gumbo.compiler.partitioner.PartitionedCUGroup;

import java.util.List;
import java.util.Set;

public interface GroupingPolicy {
	
	
	/**
	 * Groups several semijoin together in a group, 
	 * according to a certain policy.
	 * 
	 * @param semijoins the semijoins that will be partitioned
	 * @return a list of semijoin groups (sets)
	 */
	List<Set<GuardedSemiJoinCalculation>> group(Set<GuardedSemiJoinCalculation> semijoins);

}
