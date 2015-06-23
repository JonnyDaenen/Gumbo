package gumbo.compiler.grouper.policies;

import gumbo.compiler.grouper.GuardedSemiJoinCalculation;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Puts all semijoins together in one group.
 * 
 * @author Jonny Daenen
 *
 */
public class AllGrouper implements GroupingPolicy {

	
	/**
	 * Wraps all semijoin together in one group.
	 */
	@Override
	public List<Set<GuardedSemiJoinCalculation>> group(
			Set<GuardedSemiJoinCalculation> semijoins) {
		
		LinkedList<Set<GuardedSemiJoinCalculation>> groupedResult = new LinkedList<Set<GuardedSemiJoinCalculation>>();
		HashSet<GuardedSemiJoinCalculation> group = new HashSet<GuardedSemiJoinCalculation>(1);
		
		for (GuardedSemiJoinCalculation semijoin : semijoins) {	
			group.add(semijoin);
		}
		
		groupedResult.add(group);
		
		return groupedResult;
	}

}
