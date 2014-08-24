/**
 * Created: 22 Aug 2014
 */
package mapreduce.guardedfragment.planner.structures.operations;

import java.util.Set;

import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;


/**
 * interface for a map function.
 * 
 * @author Jonny Daenen
 *
 */
public abstract class GFMapper {
	
	// OPTIMIZE maybe use a pipe for output as does Hadoop
	public abstract Set<Pair<String,String>> map(String value, Set<GFExistentialExpression> formulaSet);

	/**
	 * Add a KV-pair to a resultset.
	 * @param key the key
	 * @param value the value
	 * @param resultSet the resultset to add the pair to
	 */
	protected void addOutput(String key, String value, Set<Pair<String,String>> resultSet){
		
		Pair<String, String> p = new Pair<String,String>(key, value);
		resultSet.add(p);
		
	}
}
