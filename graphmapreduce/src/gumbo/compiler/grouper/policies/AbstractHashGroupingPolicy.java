package gumbo.compiler.grouper.policies;

import gumbo.compiler.grouper.GuardedSemiJoinCalculation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;


/**
 * Abstract class used to create policies that group
 * based on a hash of the semijoin.
 * 
 * @author Jonny Daenen
 *
 */
public abstract class AbstractHashGroupingPolicy implements GroupingPolicy {
	
	/**
	 * Wraps all semijoin together in one group.
	 */
	@Override
	public List<Set<GuardedSemiJoinCalculation>> group(
			Set<GuardedSemiJoinCalculation> semijoins) {

		// hash the semijoins
		HashMap<String,Set<GuardedSemiJoinCalculation>> hashmap = hash(semijoins); 

		// each bucket becomes one group
		LinkedList<Set<GuardedSemiJoinCalculation>> groupedResult = new LinkedList<Set<GuardedSemiJoinCalculation>>();

		for (String key : hashmap.keySet()) {
			HashSet<GuardedSemiJoinCalculation> group = new HashSet<GuardedSemiJoinCalculation>(1);
			group.addAll(hashmap.get(key));
			groupedResult.add(group);
		}


		return groupedResult;
	}

	private HashMap<String, Set<GuardedSemiJoinCalculation>> hash(
			Set<GuardedSemiJoinCalculation> semijoins) {

		HashMap<String, Set<GuardedSemiJoinCalculation>> hashmap = new HashMap<String, Set<GuardedSemiJoinCalculation>>();
		
		for (GuardedSemiJoinCalculation semijoin : semijoins) {	

			String hashVal = hash(semijoin);
			if (!hashmap.containsKey(hashVal)) {
				hashmap.put(hashVal, new HashSet<GuardedSemiJoinCalculation>());
			}
			
			hashmap.get(hashVal).add(semijoin);
		}
		
		return hashmap;
	}

	protected abstract String hash(GuardedSemiJoinCalculation semijoin);


}
