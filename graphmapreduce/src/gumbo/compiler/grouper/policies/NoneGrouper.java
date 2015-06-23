/**
 * Created: 2015-06-23
 */
package gumbo.compiler.grouper.policies;

import gumbo.compiler.grouper.structures.CalculationGroup;
import gumbo.compiler.grouper.structures.GuardedSemiJoinCalculation;

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
	public List<CalculationGroup> group(
			CalculationGroup semijoins) {
		
		LinkedList<CalculationGroup> groupedResult = new LinkedList<CalculationGroup>();
		
		for (GuardedSemiJoinCalculation semijoin : semijoins.getAll()) {
			CalculationGroup group = new CalculationGroup();
			group.add(semijoin);
			groupedResult.add(group);
		}
		
		return groupedResult;
	}

}
