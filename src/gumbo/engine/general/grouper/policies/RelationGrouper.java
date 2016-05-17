package gumbo.engine.general.grouper.policies;

import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.structures.gfexpressions.GFAtomicExpression;


/**
 * Grouping policy that groups semijoin calculations that have the same guard and guarded
 * relation.
 * @author Jonny Daenen
 *
 */
public class RelationGrouper extends AbstractHashGroupingPolicy {

	@Override
	protected String hash(GuardedSemiJoinCalculation semijoin) {
		// TODO apply sorting!
		
		String all = "";
		for (GFAtomicExpression guarded : semijoin.getGuarded())
			all += guarded.getName()+";";
		
		return ""+semijoin.getGuard().getName() + ";"+all;
	}

}
