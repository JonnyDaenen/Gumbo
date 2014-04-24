package guardedfragment.structure;

import guardedfragment.structure.expressions.GFAtomicExpression;

import java.util.HashMap;
import java.util.Map;

import mapreduce.data.Tuple;

/**

 * @author Jonny Daenen
 * 
 */
public class GuardedProjection {

	GFAtomicExpression source;
	GFAtomicExpression target;
	Map<Integer, Integer> mapping; 

	/**
	 * Creates a projection to transform a guard tuple to a guarded tuple.
	 * The guard tuple must *match* the guard expression (this has to be checked by the user).
	 * 
	 * @param source the guard expression
	 * @param target the guarded expression on which to project
	 */
	public GuardedProjection(GFAtomicExpression source,
			GFAtomicExpression target) {
		
		this.source = source;
		this.target = target;

		// initialize mapping
		mapping = new HashMap<Integer, Integer>(source.getNumVariables());

		initialize();
	}
	

	private void initialize() {
		
		mapping.clear();
		
		String[] sourceVars = source.getVars();
		String[] targetVars = target.getVars();

		// TODO what if mapping is not possible? -> test using projection of the variable tuple??
		
		// check for equal variable identifiers
		// for each position of the target
		for (int i = 0; i < targetVars.length; i++) {
			// check where it occurs in the source
			for (int j = 0; j < sourceVars.length; j++) {
				
				// if we find a match
				if (targetVars[i].equals(sourceVars[j])) {
					
					// add it
					mapping.put(i, j);

					// other matches will have the same value
					// when they belong to the source GF-expression
					break;
				}
			}

		}
	}

	/**
	 * Empties the current mapping and loads the given one (by copying all
	 * entries). The mapping is from target (guarded) to source (guard). 
	 * 
	 * @param newmap the new mapping
	 */
	@Deprecated
	public void loadMapping(Map<Integer, Integer> newmap) {
		// TODO add bound control
		this.mapping.clear();
		for (int key : newmap.keySet())
			this.mapping.put(key, newmap.get(key));
	}

	/**
	 * Connect a position in the target relation to a position in the source
	 * relation.
	 * 
	 * @param source
	 *            position in the target relation
	 * @param target
	 *            position in the source relation
	 */
	public void addMapping(int source, int target) {
		// TODO add bound control
		this.mapping.put(target, source);

	}

	/**
	 * Converts a tuple from the source relation to a tuple in the target
	 * relation, according to the mapping.
	 * It is not checked if the tuple belongs to the guard expression.
	 * This is because of performance reasons.
	 * 
	 * @return a projection of a given source-tuple to the target relation
	 * @throws NonMatchingTupleException not thrown for now
	 */
	public Tuple project(Tuple t) throws NonMatchingTupleException {
		
		// TODO throw exception if non-matching
		

		String[] s = new String[target.getNumFields()];

		// copy fields one by one
		for (int i = 0; i < target.getNumFields(); i++)
			s[i] = t.get(mapping.get(i));

		// create a new tuple from the generated String
		return new Tuple(target.getName(), s);

	}

}
