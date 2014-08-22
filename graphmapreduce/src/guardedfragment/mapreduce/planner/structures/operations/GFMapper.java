/**
 * Created: 22 Aug 2014
 */
package guardedfragment.mapreduce.planner.structures.operations;

import guardedfragment.structure.gfexpressions.io.Pair;

import java.util.Set;


/**
 * interface for a map function.
 * 
 * @author Jonny Daenen
 *
 */
public abstract class GFMapper {
	
	// OPTIMIZE maybe use a pipe for output as does Hadoop
	public abstract Set<Pair<String,String>> map(String value);

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
