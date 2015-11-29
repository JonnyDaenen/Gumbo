package gumbo.engine.hadoop2.mapreduce.tools.tupleops;

import java.util.ArrayList;
import java.util.List;

import gumbo.engine.hadoop2.mapreduce.tools.QuickWrappedTuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.io.Pair;


/**
 * Tuple filter that checks equality between fields.
 * 
 * @author Jonny Daenen
 *
 */
public class EqualityFilter implements TupleFilter {

	int sizeRequirement;
	
	List<Pair<Integer,Integer>> comparisons;
	
	public EqualityFilter(int sizeRequirement) {
		this.sizeRequirement = sizeRequirement;
		comparisons = new ArrayList<>(10);
	}

	public EqualityFilter(GFAtomicExpression guard) {
		comparisons = new ArrayList<>(10);
		set(guard);
	}
	
	public EqualityFilter(byte [] eq) {
		comparisons = new ArrayList<>(10);
		set(eq);
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

	/**
	 * Overwrites the size requirement and adds new comparisons
	 * based on the equality type of the guard atom.
	 * 
	 * <b>Warning, all supplied atoms should be of the same size!</b>
	 * 
	 * @param guard a guard atom
	 */
	public void set(GFAtomicExpression guard) {
		clear();
		sizeRequirement = guard.getNumFields();
		
		String[] vars = guard.getVars();
		
		// for each var
		for (int i = 0; i < vars.length; i++) {
			String first = vars[i];
			
			// find first match, next matches will be solved by next i
			for (int j = i; j < vars.length; j++) {
				String second = vars[i];
				if (first.equals(second)) {
					add(i,j);
					break;
				}
			}

		}
		
		// FUTURE support for constants!
		
		
	}
	
	
	public void set(byte [] eq) {
		clear();
		sizeRequirement = eq.length;
		
		
		// for each var
		for (int i = 0; i < sizeRequirement; i++) {
			// find first match, next matches will be solved by next i
			for (int j = i; j < sizeRequirement; j++) {
				
				if (eq[i] == eq[j]) {
					add(i,j);
					break;
				}
				
			}

		}
		
		// FUTURE support for constants!
		
		
	}
	
}
