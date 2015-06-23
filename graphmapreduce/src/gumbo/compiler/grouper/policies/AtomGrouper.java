package gumbo.compiler.grouper.policies;

import gumbo.compiler.grouper.GuardedSemiJoinCalculation;

import java.util.List;
import java.util.Set;

/**
 * Grouping policy that groups semijoin calculations that have the same guard and guarded
 * atom. This is not very useful, as this is done in the engine as well...
 * Plus, the semijoins are in a set, so duplicates are not present anyway.
 * 
 * TODO maybe remove this feature in the engine?
 * 
 * @author Jonny Daenen
 *
 */
public class AtomGrouper extends AbstractHashGroupingPolicy {

	@Override
	protected String hash(GuardedSemiJoinCalculation semijoin) {
		return ""+semijoin.getGuard() + ";"+semijoin.getGuarded();
	}

}
