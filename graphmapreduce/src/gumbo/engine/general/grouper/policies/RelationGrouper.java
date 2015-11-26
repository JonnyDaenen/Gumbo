package gumbo.engine.general.grouper.policies;

import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;


/**
 * Grouping policy that groups semijoin calculations that have the same guard and guarded
 * relation.
 * @author Jonny Daenen
 *
 */
public class RelationGrouper extends AbstractHashGroupingPolicy {

	@Override
	protected String hash(GuardedSemiJoinCalculation semijoin) {
		return ""+semijoin.getGuard().getName() + ";"+semijoin.getGuarded().getName();
	}

}
