/**
 * Created: 21 Aug 2014
 */
package mapreduce.guardedfragment.planner.compiler.mappers;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;

/**
 * @author Jonny Daenen
 *
 */
public class GFMapper2Generic extends GFMapper implements Serializable {

	private static final long serialVersionUID = 1L;
	

	private static final Log LOG = LogFactory.getLog(GFMapper2Generic.class);

	/**
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFMapper#map(java.lang.String)
	 */
	@Override
	public Set<Pair<String, String>> map(String value, Collection<GFExistentialExpression> expressionSet) {
		
		Set<Pair<String,String>> result = new HashSet<Pair<String,String>>();
		
		if (value.contains(";")) {
			
//			if(value.startsWith("R(0,0,0,0)") || value.startsWith("R(1,1,1,1)"))
//				LOG.debug("processing: " + value);
			
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
