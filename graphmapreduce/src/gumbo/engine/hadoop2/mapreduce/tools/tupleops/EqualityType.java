package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import java.util.ArrayList;
import java.util.List;

import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.structures.gfexpressions.io.Pair;


/**
 * Tuple filter that checks equality between fields.
 * 
 * @author Jonny Daenen
 *
 */
public class EqualityType implements TupleFilter {

	int sizeRequirement;
	
	List<Pair<Integer,Integer>> comparisons;
	
	public EqualityType(int sizeRequirement) {
		this.sizeRequirement = sizeRequirement;
		comparisons = new ArrayList<>(10);
	}

	/**
	 * Checks whether the tuple conforms to the comparisons.
	 * First, a size check is performed, then, all comparisons are checked.
	 * As soon as one of these fails, false is returned. 
	 * If none fail, true is returned.
	 * 
	 * @return true iff tuple conforms to comparisons
	 */
	@Override
	public boolean check(QuickWrappedTuple qt) {
		// size check
		if (qt.size() != sizeRequirement){
			return false;
		}
		
		// all equalities should hold
		for (Pair<Integer, Integer> c : comparisons) {
			if (!qt.equals(c.fst, c.snd))
				return false;
		}
		
		return true;
	}
	
	/**
	 * Empties comparisons, size remains unchanged.
	 */
	public void clear() {
		comparisons.clear();
	}
	
	
	/**
	 * Add a comparison of position i and j.
	 * @param i first pos
	 * @param j second pos
	 */
	public void add(int i, int j) {
		comparisons.add(new Pair<>(i,j));
	}

}
