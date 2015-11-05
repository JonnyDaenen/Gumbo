package gumbo.engine.general.grouper.policies;

import gumbo.engine.general.grouper.structures.GuardedSemiJoinCalculation;

/**
 * Grouping policy that groups semijoin calculations that have the same guard and guarded
 * atom. This is not very useful, as this is done in the engine as well...
 * Plus, the semijoins are in a set, so duplicates are not present anyway.
 * 
 * TODO maybe remove this feature from the engine? (when we can do it here, the engine can be more general)
 * 
 * @author Jonny Daenen
 *
 */
public class GuardedAtomGrouper extends AbstractHashGroupingPolicy {

	@Override
	protected String hash(GuardedSemiJoinCalculation semijoin) {
		return semijoin.getGuarded().getVariableString(semijoin.getGuard().getName());
	}

}
