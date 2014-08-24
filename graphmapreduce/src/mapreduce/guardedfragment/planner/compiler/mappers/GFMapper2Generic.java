/**
 * Created: 21 Aug 2014
 */
package mapreduce.guardedfragment.planner.compiler.mappers;

import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;

/**
 * @author Jonny Daenen
 *
 */
public class GFMapper2Generic extends GFMapper{

	/**
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFMapper#map(java.lang.String)
	 */
	@Override
	public Set<Pair<String, String>> map(String value, Set<GFExistentialExpression> expressionSet) {
		
		Set<Pair<String,String>> result = new HashSet<Pair<String,String>>();
		
		if (value.contains(";")) {
			
			String[] t = value.split(new String(";"));
			if (t.length == 2) { // guarded atoms that are true
				
				// key is the guard, value is the guarded tuple
				//context.write(new Text(t[0]), new Text(t[1]));
				addOutput(t[0], t[1], result);
			
			// propagate keep alive
			} else { 
				//context.write(new Text(t[0]), new Text(new String()));
				addOutput(t[0], "", result);
			}
		}
		
		return result;
	}

}
