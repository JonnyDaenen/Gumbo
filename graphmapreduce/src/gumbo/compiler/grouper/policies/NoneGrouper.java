/**
 * Created: 2015-06-23
 */
package gumbo.compiler.grouper.policies;

import gumbo.compiler.grouper.GuardedSemiJoinCalculation;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Grouping policy that performs no grouping at all.
 * All binary Semijoins are put in a separate group.
 * 
 * @author Jonny Daenen
 *
 */
public class NoneGrouper implements GroupingPolicy {

	
	/**
	 * Wraps each semijoin in a separate group.
	 */
	@Override
	public List<Set<GuardedSemiJoinCalculation>> group(
			Set<GuardedSemiJoinCalculation> semijoins) {
		
		LinkedList<Set<GuardedSemiJoinCalculation>> groupedResult = new LinkedList<Set<GuardedSemiJoinCalculation>>();
		
		for (GuardedSemiJoinCalculation semijoin : semijoins) {
			HashSet<GuardedSemiJoinCalculation> group = new HashSet<GuardedSemiJoinCalculation>(1);
			group.add(semijoin);
			groupedResult.add(group);
		}
		
		return groupedResult;
	}

}
