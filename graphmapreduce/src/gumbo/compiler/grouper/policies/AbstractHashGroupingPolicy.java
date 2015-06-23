package gumbo.compiler.grouper.policies;

import gumbo.compiler.grouper.structures.CalculationGroup;
import gumbo.compiler.grouper.structures.GuardedSemiJoinCalculation;

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
	public List<CalculationGroup> group(
			CalculationGroup semijoins) {

		// hash the semijoins
		HashMap<String,CalculationGroup> hashmap = hash(semijoins); 

		// each bucket becomes one group
		LinkedList<CalculationGroup> groupedResult = new LinkedList<CalculationGroup>();

		for (String key : hashmap.keySet()) {
			CalculationGroup group = new CalculationGroup();
			group.addAll(hashmap.get(key));
			groupedResult.add(group);
		}


		return groupedResult;
	}

	private HashMap<String, CalculationGroup> hash(
			CalculationGroup semijoins) {

		HashMap<String, CalculationGroup> hashmap = new HashMap<String, CalculationGroup>();
		
		for (GuardedSemiJoinCalculation semijoin : semijoins.getAll()) {	

			String hashVal = hash(semijoin);
			if (!hashmap.containsKey(hashVal)) {
				hashmap.put(hashVal, new CalculationGroup());
			}
			
			hashmap.get(hashVal).add(semijoin);
		}
		
		return hashmap;
	}

	protected abstract String hash(GuardedSemiJoinCalculation semijoin);


}
