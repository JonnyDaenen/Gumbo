package gumbo.engine.general.grouper.policies;

import java.util.LinkedList;
import java.util.List;

import gumbo.engine.general.grouper.structures.CalculationGroup;
import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;

/**
 * Puts all semijoins together in one group.
 * 
 * @author Jonny Daenen
 *
 */
public class AllGrouper implements GroupingPolicy {

	
	/**
	 * Wraps all semijoins together in one group.
	 */
	@Override
	public List<CalculationGroup> group(
			CalculationGroup semijoins) {
		
		LinkedList<CalculationGroup> groupedResult = new LinkedList<CalculationGroup>();
		CalculationGroup group = new CalculationGroup(semijoins.getRelevantExpressions());
		
		for (GuardedSemiJoinCalculation semijoin : semijoins.getAll()) {	
			group.add(semijoin);
		}
		
		groupedResult.add(group);
		
		return groupedResult;
	}

}
