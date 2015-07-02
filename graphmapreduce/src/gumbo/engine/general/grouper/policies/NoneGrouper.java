/**
 * Created: 2015-06-23
 */
package gumbo.engine.general.grouper.policies;

import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;

import java.util.LinkedList;
import java.util.List;

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
