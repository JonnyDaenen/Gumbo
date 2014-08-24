/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.planner.structures.operations;

import java.util.Collection;
import java.util.Set;

import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;

/**
 * @author Jonny Daenen
 *
 */
public abstract class GFReducer {
	
	/**
	 * 
	 * @param key the group key
	 * @param values the set of values associated with the group key
	 * @param expressionSet the set of formulas that are being calculated
	 * @return A set of reduced values, where each value is assosiated with an output file.
	 * 
	 * OPTIMIZE give some kind of context to write to
	 */
	public abstract Set<Pair<String,String>> reduce(String key, Iterable<? extends Object> values, Collection<GFExistentialExpression> expressionSet);


}
