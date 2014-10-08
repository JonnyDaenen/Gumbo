/**
 * Created: 21 Aug 2014
 */
package mapreduce.guardedfragment.planner.compiler.mappers;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

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
	public Iterable<Pair<Text, Text>> map(Text v) {
		
		Set<Pair<Text,Text>> result = new HashSet<Pair<Text,Text>>();
		
		String value = v.toString();
		if (value.contains(";")) {
			
//			if(value.startsWith("R(0,0,0,0)") || value.startsWith("R(1,1,1,1)"))
//				LOG.debug("processing: " + value);
			
			// OPTIMIZE use text functions
			String[] t = value.split(";");
			if (t.length == 2) { // guarded atoms that are true
				// key is the guard, value is the guarded tuple
				//context.write(new Text(t[0]), new Text(t[1]));
				addOutput(new Text(t[0]), new Text(t[1]), result);
//				LOG.warn("M2: " + t[0] + " " + t[1]);
			
			// propagate keep alive
			} else { 
				//context.write(new Text(t[0]), new Text(new String()));
				addOutput(new Text(t[0]), new Text(""), result);
//				LOG.warn("M2: " + t[0] + " ");
			}
		}
		
		return result;
	}

}
