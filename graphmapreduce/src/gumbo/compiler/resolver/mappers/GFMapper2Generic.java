/**
 * Created: 21 Aug 2014
 */
package gumbo.compiler.resolver.mappers;

import gumbo.compiler.structures.operations.GFMapper;
import gumbo.compiler.structures.operations.GFOperationInitException;
import gumbo.guardedfragment.gfexpressions.io.Pair;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * @author Jonny Daenen
 *
 */
public class GFMapper2Generic extends GFMapper implements Serializable {

	private static final long serialVersionUID = 1L;
	

	private static final Log LOG = LogFactory.getLog(GFMapper2Generic.class);

	/**
	 * @see gumbo.compiler.structures.operations.GFMapper#map(java.lang.String)
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
	
	/**
	 * @see gumbo.compiler.structures.operations.GFMapper#map(org.apache.hadoop.io.Text, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	public void map(Text key, Text value, Context context) throws GFOperationInitException, IOException, InterruptedException {
//		LOG.warn(key.toString() + " - "+  value.toString());
		context.write(key, value);
	}

}
