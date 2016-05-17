package gumbo.engine.general.grouper.policies;

import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;
import gumbo.structures.gfexpressions.GFAtomicExpression;

/**
 * 
 * @author Jonny Daenen
 *
 */
public class GuardedAtomGrouper extends AbstractHashGroupingPolicy {

	@Override
	protected String hash(GuardedSemiJoinCalculation semijoin) {
		// TODO why do we need the variables? they are irrelevant for different guards...
		String all = "";
		for (GFAtomicExpression guarded : semijoin.getGuarded())
			all += guarded.getVariableString(guarded.getName());
		return all;
	}

}
