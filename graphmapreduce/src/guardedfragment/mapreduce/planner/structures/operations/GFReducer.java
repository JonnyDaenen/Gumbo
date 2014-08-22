/**
 * Created: 22 Aug 2014
 */
package guardedfragment.mapreduce.planner.structures.operations;

import guardedfragment.structure.gfexpressions.io.Pair;

import java.util.Set;

/**
 * @author Jonny Daenen
 *
 */
public abstract class GFReducer {
	
	public abstract Set<String> reduce(String key, Set<String> values);


}
