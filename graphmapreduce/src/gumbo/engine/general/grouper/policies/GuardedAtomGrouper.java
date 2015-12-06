package gumbo.engine.general.grouper.policies;

import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;

/**
 * 
 * @author Jonny Daenen
 *
 */
public class GuardedAtomGrouper extends AbstractHashGroupingPolicy {

	@Override
	protected String hash(GuardedSemiJoinCalculation semijoin) {
		// TODO why do we need the variables? they are irrelevant for different guards...
		return semijoin.getGuarded().getVariableString(semijoin.getGuarded().getName());
	}

}
