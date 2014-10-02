/**
 * Created: 21 Aug 2014
 */
package mapreduce.guardedfragment.planner.compiler.mappers;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import mapreduce.guardedfragment.planner.structures.data.Tuple;
import mapreduce.guardedfragment.planner.structures.operations.GFMapper;
import mapreduce.guardedfragment.planner.structures.operations.GFOperationInitException;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.Pair;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import mapreduce.guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Also outputs the atoms when a guard is projected onto them.
 * 
 * @author Jonny Daenen
 *
 */
public class GFMapper1AtomBased extends GFMapper implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(GFMapper1AtomBased.class);

	

	
	/**
	 * @throws GFOperationInitException 
	 * @see mapreduce.guardedfragment.planner.structures.operations.GFMapper#map(java.lang.String)
	 */
	@Override
	public Iterable<Pair<String,String>> map(String value) throws GFOperationInitException {
		return new GFMapper1AtomBasedIterator(getGuardsAll(), getGuardedsAll(), getGGPairsAll(), value);
	}
	

}
