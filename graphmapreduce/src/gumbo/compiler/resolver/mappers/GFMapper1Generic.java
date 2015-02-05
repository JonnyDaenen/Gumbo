/**
 * Created: 21 Aug 2014
 */
package gumbo.compiler.resolver.mappers;

import gumbo.compiler.structures.data.Tuple;
import gumbo.compiler.structures.operations.GFMapper;
import gumbo.compiler.structures.operations.GFOperationInitException;
import gumbo.guardedfragment.gfexpressions.GFAtomicExpression;
import gumbo.guardedfragment.gfexpressions.GFExistentialExpression;
import gumbo.guardedfragment.gfexpressions.io.Pair;
import gumbo.guardedfragment.gfexpressions.operations.GFAtomProjection;
import gumbo.guardedfragment.gfexpressions.operations.NonMatchingTupleException;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;

/**
 * @author Jonny Daenen
 *
 */
public class GFMapper1Generic extends GFMapper implements Serializable {
	
	private static final long serialVersionUID = 1L;
	private static final Log LOG = LogFactory.getLog(GFMapper1Generic.class);

	

	
	/**
	 * @throws GFOperationInitException 
	 * @see gumbo.compiler.structures.operations.GFMapper#map(java.lang.String)
	 */
	@Override
	public Iterable<Pair<Text,Text>> map(Text value) throws GFOperationInitException {
		return new GFMapper1Iterator(getGuardsAll(), getGuardedsAll(), getGGPairsAll(), value);
	}
	
	/**
	 * Original implementation of map1
	 * @param value
	 * @return
	 * @throws GFOperationInitException
	 */
	@Deprecated
	public Iterable<Pair<Text,Text>> map_old(String value) throws GFOperationInitException {
		
		Set<Pair<Text,Text>> result = new HashSet<Pair<Text,Text>>();
		
		for (GFExistentialExpression formula : expressionSet) {

			// convert value to tuple
			Tuple t = new Tuple(value.toString());
//			LOG.trace("An original value: " + value.toString());

			GFAtomicExpression guard = formula.getGuard();
			Collection<GFAtomicExpression> guardedRelations = getGuardeds(formula); // these are precalculated

			// check if tuple matches guard
			if (guard.matches(t)) {

				// output guard:guard
				//context.write(new Text(t.toString()), new Text(t.toString()));
				addOutput(new Text(t.toString()), new Text(t.toString()), result);
				
//				LOG.trace("The first Mapper outputs the pair: " + t.toString() + " : " + t.toString());

				for (GFAtomicExpression guarded : guardedRelations) {

					// get projection
					// OPTIMIZE do this when initializing
					GFAtomProjection gp = new GFAtomProjection(guard, guarded);
					Tuple tprime;
					try {
						// project to key
						tprime = gp.project(t);

						if (guarded.matches(tprime)) {

							// LOG.error(tprime + " matches' " + guarded +
							// " val: " + t);
							// project to key and write out
							//context.write(new Text(tprime.toString()), new Text(t.toString()));
							addOutput(new Text(tprime.toString()),new Text(t.toString()), result);
//							LOG.trace("The first Mapper outputs the pair: " + tprime.toString() + " : " + t.toString());

						}
					} catch (NonMatchingTupleException e1) {
						// should not happen!
						e1.printStackTrace();
					}

				}

			}

			
			// ATTENTION: do not use ELSE here
			// the guard can appear as guarded too!
			
			// check if tuple matches a guarded atom
			for (GFAtomicExpression guarded : guardedRelations) {

				// if so, output tuple with same key and value
				if (guarded.matches(t)) {
					// LOG.error(t + " matches " + guarded);
					//context.write(new Text(t.toString()), new Text(t.toString()));
					addOutput(new Text(t.toString()), new Text(t.toString()), result);
//					LOG.trace("The first Mapper outputs the pair: " + t.toString() + " : " + t.toString());
					// break;
				}
			}

		}
		
		return result;
		
	}

}
