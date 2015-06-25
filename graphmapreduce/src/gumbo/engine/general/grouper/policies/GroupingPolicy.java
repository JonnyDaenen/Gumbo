package gumbo.engine.general.grouper.policies;

import gumbo.compiler.partitioner.PartitionedCUGroup;
import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;

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
	List<CalculationGroup> group(CalculationGroup group);

}
