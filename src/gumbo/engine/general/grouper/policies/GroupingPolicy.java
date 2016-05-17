package gumbo.engine.general.grouper.policies;

import java.util.List;

import gumbo.engine.general.grouper.GroupingException;
import gumbo.engine.general.grouper.structures.CalculationGroup;

/**
 * Strategy for grouping a set of semijoins.
 * 
 * @author Jonny Daenen
 *
 */
public interface GroupingPolicy {
	
	
	/**
	 * Groups several semijoin together in a group, 
	 * according to a certain policy.
	 * 
	 * @param semijoins the semijoins that will be partitioned
	 * @return a list of semijoin groups (sets)
	 * @throws GroupingException 
	 */
	List<CalculationGroup> group(CalculationGroup group) throws GroupingException;

}
