package gumbo.compiler.grouper.policies;

import gumbo.compiler.grouper.GuardedSemiJoinCalculation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


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
